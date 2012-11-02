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
    'bufferSize' => 1000,      // buffer size
    'autoIndex'  => true,      // ensure indexes automatically
    'extraExpiredSec' => 60    // job block time. jobの大まかな処理時間.
  );

  protected static $prefixMC = 'JobQueue_';
  private $log;
  protected $opts;

  /**
   * count     : job quantity
   * jobs      : array( $label=>array( array('opid'=>string, 'value'=>mixed $value, 'opts'=>array() ), ... ) )
   */
  protected $bufferDatas = array('count'=>0, 'jobs'=>array() );
  protected $bufferSize;
  protected $buffered = false;

  protected $mdb;
  protected $mcJobQueue;

  public function __construct(MongoDB $mongoDB, array $opts=array()) {
    $this->log = Logger::getLogger(__CLASS__);

    $defaultOpts = self::$defaultOptions;
    $mergedOpts = array();
    foreach($defaultOpts as $k => $v) {
      $mergedOpts[$k] = isset($opts[$k]) ? $opts[$k]: $v;
      unset($opts[$k]);
    }

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
    $this->log->debug('call');
    $mc = $this->mcJobQueue;
    $opts = array('background'=>true);
    $mc->ensureIndex(array('priority'=>1, '_id'=>1), $opts);
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

    if(!array_key_exists($label, $this->bufferDatas['jobs'])) {
      $this->bufferDatas['jobs'][$label] = array();
    }

    $this->bufferDatas['jobs'][$label][] = array('opid' => $opid, 'value'=>$value, 'opts'=>$mergedOpts);
    $this->bufferDatas['count']++;

    if($this->log->isTraceEnabled()){
      $cnt = count($this->bufferDatas['jobs'][$label]);
      $this->log->trace(
        sprintf('call enqueue label:%s, jobs:%d', $label, $cnt)
      );
    }

    $this->flush();
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
    /**
     * flushされる条件はjobsの合計が閾値を超えたときのみ
     *
     */
    $force = $force === true;

    if(!$this->isBuffer()) $force = true;

    if(!$force){
      if($this->bufferDatas['count'] < $this->bufferSize){
          return;
      }
    }

    $jobs = array();
    foreach($this->bufferDatas['jobs'] as $label => $labelledJobs){
      foreach($labelledJobs as $job) {
        $jobs[] = array(
          'label'         => $label,
          'value'         => $job['value'],
          'priority'      => $job['opts' ]['priority'],
          'opid'          => $job['opid'],
          'lockBy'        => null,
          'lockExpiredAt' => new MongoDate()
        );
      }
      if($this->log->isTraceEnabled()){
        $cnt = count($labelledJobs);
        $this->log->trace(sprintf('to be flushed datas. label:[%s] qty:%d', $label, $cnt));
      }
    }
    if(!empty($jobs)){
      $this->batchInsert($jobs);
    }
    // cleanup
    $this->bufferDatas['jobs'     ] = array();
    $this->bufferDatas['count'    ] = 0;
  }



  protected function batchInsert($jobs){
    $opts = array('safe'=>true);
    $this->mcJobQueue->batchInsert($jobs, $opts);
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
  public function findJob($opid, array $labels=array(), array $opts=array()) {
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

    if(!is_array($labels))
      throw new InvalidArgumentException('$labels is only accept array value.');

    foreach($labels as $label)
      if(!is_string($label))
        throw new InvalidArgumentException('$labels should contain to accept string value. : ' . gettype($label) );

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
function(mcName, labels, extraMSec, uuid){
  var now, q, u, s;
  now=new Date();
  q={};
  if (labels.length > 0) q.label = {$in:labels};
  q.lockExpiredAt = {$lt:now};
  u={$set:{lockExpiredAt:new Date(now.getTime() + extraMSec),lockBy:uuid}};
  s={priority:1};
  return db[mcName].findAndModify({query:q, update:u, sort:s, new:true});
}
EOD;
    $args = array(
      $this->getName(),
      $labels,
      $extraExpiredSec * 1000,// 秒をミリ秒に変換
      $opid
    );
    $v = $this->mdb->execute($code, $args);
    if(@$v['ok'] != 1) throw new JobQueueException('Mongo error : ' . var_export($v, true));
    if(is_null($v['retval'])) return null;
    $job = new Job($this->mcJobQueue, $v['retval']);
    return $job;
  }



}

