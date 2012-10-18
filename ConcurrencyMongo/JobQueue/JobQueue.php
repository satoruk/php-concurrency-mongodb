<?php

namespace ConcurrencyMongo\JobQueue;

use Exception;
use InvalidArgumentException;
use MongoDB;
use MongoDate;
use Logger;

class JobQueue {

  /*
   *
   * extraExpiredSec : JobQueue上のラベル毎のJobの数多い場合が指定時間停止
   */
  public static $defaultOptions = array(
    'name'       => 'default', // collection name
    'priority'   => 5,         // priority, 0 is most high priority
    'bufferSize' => 100,       // buffer size
    'autoIndex'  => false,     // ensure indexes automatically
    'extraExpiredSec' => 60    // 失効延長時間(秒) 失効した場合 expiredWaitがコールされる.
  );

  protected static $prefixMC = 'JobQueue_';
  private $log;
  protected $opts;

  /**
   * array( $label=>array( 'expired'=>integer $expiredTime, 'jobs'=>array( array('opid'=>string, 'value'=>mixed $value, 'opts'=>array() ), ... ) ), ... )
   */
  protected $bufferDatas = array();
  protected $bufferSize;
  protected $buffered = false;

  protected $mdb;
  protected $mcJobQueue;

  public function __construct(MongoDB $mongoDB, $opts=array()) {
    $this->log = Logger::getLogger(__CLASS__);

    $defaultOpts = self::$defaultOptions;
    $mergedOpts = array();
    foreach($defaultOpts as $k => $v) {
      $mergedOpts[$k] = isset($opts[$k]) ? $opts[$k]: $v;
      unset($opts[$k]);
    }

    if(!($mongoDB instanceof MongoDB))
      throw new InvalidArgumentException('$mongodb is not MongoDB instance.');

    if(!empty($opts))
      throw new InvalidArgumentException('Unknown opts keys : ' . implode(' and ',array_keys($opts)));

    // integer
    foreach (array('bufferSize', 'priority', 'extraExpiredSec') as $k)
      if(!is_integer($mergedOpts[$k]))
        throw new InvalidArgumentException(sprintf('%s of $opts is only accept integer value.', $k));

    // string
    foreach (array('name') as $k)
      if(!is_string($mergedOpts[$k]))
        throw new InvalidArgumentException(sprintf('%s of $opts is only accept string value.', $k));

    // bool
    foreach (array('autoIndex') as $k)
      if(!is_bool($mergedOpts[$k]))
        throw new InvalidArgumentException(sprintf('%s of $opts is only accept boolean value.', $k));

    $this->opts = $mergedOpts;
    $this->bufferSize = intval($mergedOpts['bufferSize']);
    $this->extraExpiredSec = intval($mergedOpts['extraExpiredSec']);
    $this->mdb = $mongoDB;
    $this->mcJobQueue = $mongoDB->selectCollection(self::$prefixMC . $mergedOpts['name']);

    if($mergedOpts['autoIndex']===true)
      $this->ensureIndex();

    if($this->log->isTraceEnabled()){
      $this->log->trace(sprintf('bufferSize:%d', $this->bufferSize));
    }
  }



  public function getName(){
    return $this->mcJobQueue->getName();
  }



  public function ensureIndex() {
    $this->log->warn('this method is pending');
    // TODO
    //$this->mcJobQueue->ensureIndex(array('opid'=>1,'label'=>1), array('safe'=>true));// 
  }



  public function countJob($opid=null){
    $query = array();
    if(!is_null($opid)){
      if(!is_string($opid)) throw new InvalidArgumentException('$opid is only accept string value.');
      $query['opid'] = $opid;
    }
    $cnt = $this->mcJobQueue->count($query);
    return $cnt;
  }



  public function countLabel($label, $opid=null){
    $query = array();
    if(!is_string($label)) throw new InvalidArgumentException('$label is only accept string value.');
    $query['label'] = $label;
    if(!is_null($opid)){
      if(!is_string($opid)) throw new InvalidArgumentException('$opid is only accept string value.');
      $query['opid'] = $opid;
    }
    $cnt = $this->mcJobQueue->count($query);
    return $cnt;
  }



  public function enqueue($opid, $label, $value, $opts=array()){
    $defaultOpts = array(
      'priority' => $this->opts['priority']
    );

    $mergedOpts = array();
    foreach($defaultOpts as $k => $v) {
      $mergedOpts[$k] = isset($opts[$k]) ? $opts[$k]: $v;
      unset($opts[$k]);
    }

    /*
     * Validations
     */

    if(!is_string($opid))
      throw new InvalidArgumentException('$opid is only accept string value.');

    if(!is_string($label))
      throw new InvalidArgumentException('$label is only accept string value.');

    if(is_null($value))
      throw new InvalidArgumentException('$value is not null.');

    if(!empty($opts))
      throw new InvalidArgumentException('Unknown opts keys : ' . implode(' and ',array_keys($opts)));

    // integer
    foreach (array('priority') as $k)
      if(!is_integer($mergedOpts[$k]))
        throw new InvalidArgumentException(sprintf('%s of $opts is only accept integer value.', $k));

    if(!array_key_exists($label, $this->bufferDatas)) {
      $this->beforeCreateBuffer($label);
      $extraExpiredSec4Buffer = 1;// 1秒でFlushするようにする
      $this->bufferDatas[$label] = array('jobs'=>array(), 'expired'=>time()+$extraExpiredSec4Buffer);
    }

    $this->bufferDatas[$label]['jobs'][] = array('opid' => $opid, 'value'=>$value, 'opts'=>$mergedOpts);

    if($this->log->isTraceEnabled()){
      $this->log->trace(sprintf('call enqueue label:%s, jobs:%d', $label, count($this->bufferDatas[$label]['jobs'])));
      //      $this->log->debug($this->bufferDatas);
    }

    $this->flush();
  }



  protected function beforeCreateBuffer($label){
    // jobの欠損を防ぐためバッファ作成時にJobQueueの空きを確認する.
    $expired = time() + $this->extraExpiredSec;
    $threshold = $this->bufferSize * 10;// JobQueueの上限の閾値を計算 TODO 設定できる方がいいか？
    $secPrev = 0;
    $sec = 1;
    while(true){
      $cnt = $this->mcJobQueue->count(array('label'=>$label));
      if($cnt < $threshold) break;
      if($expired <= time()){
        $this->expiredWait($label);
        break;
      }
      $tmp = $sec;
      $sec = $sec + $secPrev;
      $secPrev = $tmp;
      // 余分に待ちすぎない様調整
      $now = time();
      if($expired < $now + $sec){
        $sec = $expired - $now;
      }
      if($this->log->isTraceEnabled()){
        $this->log->trace(sprintf('label:%s cnt:%d threshold:%d sleepSec:%d', $label, $cnt, $threshold, $sec));
      }
      // wait
      $tmp = $now + $sec;
      while($tmp > time()) $this->wait();
    }
  }



  protected function expiredWait($label){
    $this->log->warn(sprintf('expired wait, label %s of %s is still full.', $label, $this->getName()));
  }


  /**
   * 待ち時間を利用する目的で割り込みさせる場合このメソッドをオーバライド
   */
  protected function wait(){
    sleep(1);
  }



  public function isBuffer(){
    return $this->buffered;
  }



  public function enableBuffer(){
    $this->buffered=true;
  }



  public function disableBuffer(){
    $this->buffered=false;
    $this->flush(true);
  }



  public function flush($force=false){
    $force = $force === true;
    $flushedLabels = array();
    $tmpJobs = array();
    foreach($this->bufferDatas as $label => $datas){
      $jobs = $datas['jobs'];
      $cnt = count($jobs);
      if($cnt <= 0) continue;
      $forceByLabel = $force === true;
      // force is true if buffer was expired
      if($datas['expired'] <= time()){
        $forceByLabel = true;
        if($this->log->isTraceEnabled()){
          $this->log->trace(sprintf('buffer datas expired, so force flush. label:%s ', $label));
        }
      }
      if(!$forceByLabel and $this->isBuffer() and $cnt < $this->bufferSize) continue;
      foreach($jobs as $job) {
        $tmpJobs[] = array(
          'label'         => $label,
          'value'         => $job['value'],
          'priority'      => $job['opts' ]['priority'],
          'opid'          => $job['opid'],
          'lockBy'        => null,
          'lockExpiredAt' => new MongoDate()
        );
      }
      $flushedLabels[] = $label;
      if($this->log->isTraceEnabled()){
        $this->log->trace(sprintf('to be flushed datas. qty:%d label:%s force:%s', $cnt, $label, var_export($force, true)));
      }
    }
    // cleanup
    foreach($flushedLabels as $label){
      unset($this->bufferDatas[$label]);
    }
    if(!empty($tmpJobs)){
      $this->batchInsert($tmpJobs);
    }
  }



  protected function batchInsert($jobs){
    $this->mcJobQueue->batchInsert($jobs);
    if($this->log->isDebugEnabled()){
      $this->log->debug(sprintf('insert jobs qty:%d', count($jobs)));
    }
  }



  /**
   *
   *
   * @param $opid lock operation id
   * @param $label find form JobQueue
   * @param $opts
   * @return Jobがある場合はJobを、無い場合nullを返す
   */
  public function findJob($opid, $label, $opts=array()) {
    $this->log->debug('call');

    $defaultOpts = array(
      'extraExpiredSec' => $this->opts['extraExpiredSec']
    );

    $mergedOpts = array();
    foreach($defaultOpts as $k => $v) {
      $mergedOpts[$k] = isset($opts[$k]) ? $opts[$k]: $v;
      unset($opts[$k]);
    }

    /*
     * Validations
     */

    if(!is_string($opid))
      throw new InvalidArgumentException('$opid is only accept string value.');

    if(!is_string($label))
      throw new InvalidArgumentException('$label is only accept string value.');

    if(!empty($opts))
      throw new InvalidArgumentException('Unknown opts keys : ' . implode(' and ',array_keys($opts)));

    // integer
    foreach (array('extraExpiredSec') as $k)
      if(!is_integer($mergedOpts[$k]))
        throw new InvalidArgumentException(sprintf('%s of $opts is only accept integer value.', $k));

    //
    $extraExpiredSec = $mergedOpts['extraExpiredSec'];

    /*
     * Jobの検索
     */
    $code = <<<'EOD'
function(mcName, label, extraMSec, uuid){
  var now, q, u, s;
  now=new Date();
  q={label:label,lockExpiredAt:{$lt:now}};
  u={$set:{lockExpiredAt:new Date(now.getTime() + extraMSec),lockBy:uuid}};
  s={priority:1,_id:1};
  return db[mcName].findAndModify({query:q, update:u, sort:s, new:true});
}
EOD;
    $args = array(
      $this->getName(),
      $label,
      $extraExpiredSec * 1000,// ミリ秒に変換
      $opid
    );
    $v = $this->mdb->execute($code, $args);
    if(@$v['ok'] != 1) throw new JobQueueException('Mongo error : ' . var_export($v, true));
    if(is_null($v['retval'])) return null;
    $job = new Job($this->mcJobQueue, $v['retval']);
    return $job;
  }



}

