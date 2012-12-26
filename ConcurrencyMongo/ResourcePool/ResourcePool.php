<?php

namespace ConcurrencyMongo\ResourcePool;

use InvalidArgumentException;
use MongoDB;
use Logger;
use ConcurrencyMongo\ResourcePool\ResourceException;
use ConcurrencyMongo\ResourcePool\ExpiredResourceException;
use ConcurrencyMongo\ResourcePool\ResourceData;

/**
 * 並列リソースプール
 * 
 * MongoDBを利用してリソースの専有をします.
 * 専有方法は制限時間での悲観的ロックでトランザクションを行います.
 * 
 * extraSec     PHPプロセス内部での延長時間
 * extraSecRate MongoDB上でのリソース延長時間を計算するための値
 * 
 * 例えばextraSec:120, extraSecRate:0.5で3:00丁度だとすると
 * リソースの悲観的ロックは以下のようになります.
 * 調整時間 [sec] 120sec * 0.5 = 60sec
 *       PHP上での更新ロック時間 [extraSecLock] 3:01 (=3:00 +  60sec)
 *          悲観的ロック更新時間 [extraSecIn  ] 3:03 (=3:00 + 120sec + 60sec)
 * MongoDB上での悲観的ロック時間 [extraSecEx  ] 3:04 (=3:00 + 120sec + 60sec + 60sec)
 * 
 * PHPとMongoDBの差の間にプールへのアクセスがあった場合に自動延長が行われます.
 * 上記の例では3:01から3:03の間にプールへのアクセスがあった場合に自動延長されます.
 * 3:00から3:01は頻繁に延長処理が走らないよう猶予時間になります.
 * 
 */
class ResourcePool
{

    public static $statusInFree = 0;
    public static $statusInUse = INF;

    protected static $messageAlreadyExpired = 'This resource is already expired.';
    protected static $prefixMC = 'resourcePool_';

    private $log;

    protected $name;
    protected $uuid;
    protected $extraSec;
    protected $extraSecRate;

    protected $extraSecLock;// updateのロック時間(PHP)
    protected $extraSecIn;  // 内部リソースの専有延長時間(PHP)
    protected $extraSecEx;  // 外部リソースの専有延長時間(MongoDB)

    protected $expiredAtLock;
    protected $expiredAtIn;
    protected $expiredAtEx;

    protected $mdb;
    protected $mcDatas;
    protected $mcPools;

    // リソースデータのリスト
    // リソースデータのキー
    // If the status greater than time(), this resource data is locked.
    // {
    //   status : (ResourcePool:$statusInFree, ResourcePool::$statusInUse or integer),
    //   data   : {
    //     _id      : $mongo_id,
    //     resource : $resourceValue
    //   }
    // }
    protected $list;



    public function __construct(MongoDB $mongoDB, $name, array $opts = array())
    {
        static $logName = null;
        if (null===$logName) {
            $logName = str_replace('\\', '.', __CLASS__);
        }
        $this->log = Logger::getLogger($logName);

        $this->name = $name;
        $name = self::$prefixMC . $name;

        $defaultOpts = array(
            'uuid'         => uniqid('default-', true),
            'extraSec'     => 180, // 3 min; 専有延長時間
            'extraSecRate' => 0.2  // 余剰時間を計算する元
        );
        foreach ($defaultOpts as $k => $v) {
            $this->$k = isset($opts[$k]) ? $opts[$k]: $v;
            unset($opts[$k]);
        }

        if (!empty($opts) ) {
            throw new InvalidArgumentException('Unknown opts keys : ' . implode(' and ', array_keys($opts)));
        }

        $v = $this->extraSec;
        if (!(is_int($v) && $v >= 1)) {// 内部延長時間の計算で1以上必要
            throw new InvalidArgumentException('The extraSec of opts have to greater than or equal to 1 and integer');
        }

        $v = $this->extraSecRate;
        if (!(is_float($v) && $v >= 0.1 && $v <= 1.0)) {
            throw new InvalidArgumentException('The extraSecRate of opts have to in 0.1-1.0');
        }

        $sec = ceil($this->extraSec * $this->extraSecRate);
        $this->extraSecLock = $sec;
        $this->extraSecIn   = $sec + $this->extraSec;
        $this->extraSecEx   = $sec + $this->extraSec + $sec;

        $this->list = array();
        $this->mdb = $mongoDB;
        $this->mcDatas = $this->mdb->selectCollection($name . '_datas');
        $this->mcPools = $this->mdb->selectCollection($name . '_pools');
        $this->checkin();
    }



    public function __destruct()
    {
        try {
            $this->checkout();
        } catch (ResourceException $e) {
        }
    }



    public function getUUID()
    {
        return $this->uuid;
    }



    public function getAllocatedSize()
    {
        return count($this->list);
    }



    public function unlock()
    {
        $this->expiredAtLock = 0;
    }



    /**
     * リソースプーリングに利用しているコレクションの削除
     */
    public function drop()
    {
        foreach (array('mcDatas', 'mcPools') as $mc) {
            $this->$mc->drop();
            $this->$mc = null;
        }
    }



    /**
     * 並列リソースプーリングにエントリーして他のプールに知らせる.
     */
    protected function checkin()
    {
        $now = time();
        $this->expiredAtIn   = $now + $this->extraSecIn;
        $v = $this->mdb->execute(
            'function(mcPool, uuid, msec) { var now = new Date(); return db[mcPool].update({_id:uuid}, {$set:{createdAt:now, expiredAt:new Date(now.getTime() + msec)}}, true);}',
            array($this->mcPools->getName(), $this->uuid, 1000 * $this->extraSecEx)
        );
        if (@$v['ok'] !== 1.0) {
            throw new Exception('Mongo error : ' . var_export($v, true));
        }
        $this->clean();
    }



    /**
     * 並列リソースプールからの脱退
     */
    protected function checkout()
    {
        if (is_null($this->mcPools)) {
            return;
        }
        $this->mcPools->remove(array('_id' => $this->uuid));
        $this->clean();
    }



    /**
     * リソースの定義
     * 重複するリソースは上書きされます.
     */
    public function def($resource)
    {
        $v = $this->mcDatas->update(
            array('resource' => $resource),
            array('$set' => array('resource' => $resource)),
            array('upsert' => true)
        );
        $this->unlock();// 定義した際はロックをいったん解除
        return $v;
    }



    /**
     * 不要なリソースの削除
     */
    public function clean()
    {
        $this->update();
        $v = $this->mdb->execute(
            'function(mcPool) {return db[mcPool].remove({expiredAt:{$lt:new Date()}});}',
            array($this->mcPools->getName())
        );
        if (@$v['ok'] !== 1.0) {
            throw new Exception('Mongo error : ' . var_export($v, true));
        }
    }



    /**
     * リソースの延長処理
     */
    public function update()
    {
        $now = time();
        if ($now <= $this->expiredAtLock) {
            return;
        }
        if ($now > $this->expiredAtIn) {
            throw new ExpiredResourceException(self::$messageAlreadyExpired);
        }
        $this->expiredAtLock = $now + $this->extraSecLock;
        $this->expiredAtIn   = $now + $this->extraSecIn;
        $v = $this->mdb->execute(
            'function(mcPool, uuid, msec) { return db[mcPool].update({_id:uuid},{$set:{expiredAt:new Date(new Date().getTime() + msec)}});}',
            array($this->mcPools->getName(), $this->uuid, 1000 * $this->extraSecEx)
        );
        if (@$v['ok'] !== 1.0) {
            throw new Exception('Mongo error : ' . var_export($v, true));
        }
        $this->allocate();
    }



    /**
     * リソースの割当
     */
    protected function allocate()
    {
        // 自分の割当個数を再計算する
        //  retval.toRelease リソースが多く割り当てられていたら、この数分解放する.
        $code = <<<'EOD'
function(mcPool, mcData, uuid, isArviter) {
  var validPools, poolQty, dataQty, expQty, n, uuids, tmp, retval;
  validPools = db[mcPool].find({expiredAt:{$gt:new Date()}});
  poolQty = validPools.count();
  dataQty = db[mcData].count();
  expQty = isArviter ?  Math.ceil(dataQty / poolQty) : Math.floor(dataQty / poolQty);
  n = expQty - db[mcData].count({uuid:uuid});
  if(n <= 0 || !isFinite(n)){
    return {toRelease:-n};
  }
  uuids = validPools.map(function(v){return v._id});
  retval = [];
  while(n-- > 0) {
    tmp = db[mcData].findAndModify({query:{uuid:{$nin:uuids}},update:{$set:{uuid:uuid}}});
    if (tmp === null) {
      break;
    }
    retval.push(tmp);
  }
  return retval;
}
EOD;
        $v = $this->mdb->execute(
            $code,
            array(
                $this->mcPools->getName(),
                $this->mcDatas->getName(),
                $this->uuid,
                $this->isArbiter()
            )
        );
        if (@$v['ok'] !== 1.0) {
            throw new Exception('Mongo error : ' . var_export($v, true));
        }

        // 総リソース数
        //$totalQty = $this->mcDatas->count();// TODO 無効なリソースは除外する(Not 利用中)

        if (isset($v['retval']['toRelease'])) {
            // 多くリソースが割り当てられていた場合の解放処理
            $now = time();
            $c = (int)$v['retval']['toRelease'];
            foreach ($this->list as $k => &$val) {
                if ($c <= 0) {
                    break;
                }
                if ($val['status'] < $now) {
                    $c--;
                    // mongoのリソースからuuidを削除
                    $this->mcDatas->update(
                        array('_id', $val['data']['_id']),
                        array('$unset' => array('uuid'=>1)),
                        array('multiple' => true)
                    );
                    unset($this->list[$k]);
                }
            }
        } else {
            foreach ($v['retval'] as $val) {
                $this->list[] = array(
                    'status' => self::$statusInFree,
                    'data'   => array('_id' => $val['_id'], 'resource' => $val['resource'])
                );
            }
        }
    }



    /**
     * 自プールが調停者か否か
     * 調停者の場合、並列リソースプールの中で余っているリソースを
     * 利用することが出来る.
     */
    public function isArbiter()
    {
        // TODO あとで実装
        return false;
    }



    /**
     * 利用可能な自プールの全リソースの取得
     */
    public function getAll()
    {
        $this->update();
        $now = time();
        $list = array();
        foreach ($this->list as &$v) {
            if ($v['status'] < $now) {
                $v['status'] = self::$statusInUse;
                $list[] = new ResourceData($this, $v);
            }
        }
        return $list;
    }



    /**
     * 利用可能な自プールのリソースの取得
     */
    public function get()
    {
        $this->update();
        $now = time();
        $list = array();
        foreach ($this->list as &$v) {
            if ($v['status'] < $now) {
                $list[] = &$v;
            }
        }
        if (!empty($list)) {
            $v = &$list[rand(0, count($list) - 1)];
            $v['status'] = self::$statusInUse;
            return new ResourceData($this, $v);
        }
        return null;
    }



    /**
     * 未割当のリソースがある場合true
     */
    public function hasFree()
    {
        $this->update();
        $now = time();
        foreach ($this->list as &$v) {
            if ($v['status'] < $now) {
                return true;
            }
        }
        return false;
    }



    /**
     *
     * @param array $opts  verbose key is boolean
     */
    public function status(array $opts = array())
    {
        $now = time();
        $cntFree = 0;
        $cntUsing = 0;
        $cntBlockeds = array('1m'=>0, '5m'=>0, '15m'=>0, 'over'=>0);
        foreach ($this->list as &$v) {
            if ($v['status'] < $now) {
                $cntFree++;
            } elseif ($v['status'] === self::$statusInUse) {
                $cntUsing++;
            } else {
                $expSec = $v['status'] - $now;
                $idx = 'over';
                if ($expSec <= 60) {
                    $idx = '1m';
                } elseif ($expSec <=  5 * 60) {
                    $idx = '5m';
                } elseif ($expSec <= 15 * 60) {
                    $idx = '15m';
                }
                $cntBlockeds[$idx]++;
            }
        }

        $total = count($this->list);
        $title = sprintf('[ Resource Pool Status : %s ]', $this->name);
        $stat = sprintf(
            '%4s total, %4s free, %4s us, %4s block (in 1m:%4s, 5m:%4s, 15m:%4s, over:%d)',
            $total,
            $cntFree,
            $cntUsing,
            $total - $cntFree - $cntUsing,
            $cntBlockeds['1m'],
            $cntBlockeds['5m'],
            $cntBlockeds['15m'],
            $cntBlockeds['over']
        );

        if (@$opts['verbose'] === true) {
            $str = '--' . $title . '-------------' . PHP_EOL;
            $str .= '  ' . $stat . PHP_EOL;
            $str .= 'verbose' . PHP_EOL;
            foreach ($this->list as $k => $v) {
                if ($v['status'] < $now) {
                    $status = 'FREE';
                } elseif ($v['status'] === self::$statusInUse) {
                    $status = 'USING';
                } else {
                    $status = 'BLOCKED';
                }
                $resource = json_encode($v['data']['resource']);
                $str .= sprintf('  [%-7s] %s', $status, $resource) . PHP_EOL;
            }
            $str .= '-----------------------------------------------' . PHP_EOL;
        } else {
            $str = $title . ' ' . $stat;
        }
        return $str;
    }
}
