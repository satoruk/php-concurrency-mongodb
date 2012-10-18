<?php

namespace ConcurrencyMongo\JobQueue;

use Exception;
use InvalidArgumentException;
use MongoDB;
use MongoDate;
use Logger;

class JobQueueProducer {

  protected static $prefixMC = 'JobQueue_';

  private $log;
  private $cache = array();

  protected $opts;
  protected $func;
  protected $terminated = false;
  protected $buffered = false;
  protected $bufferSize;

  /**
   * array( $label=>array( 'expired'=>integer $expiredTime, 'jobs'=>array( array('value'=>mixed $value, 'opts'=>array() ), ... ) ), ... )
   */
  protected $bufferDatas = array();
  protected $extraExpiredSec;
  protected $mcJobQueue;

  public function __construct($opts=array()) {
    $this->log = Logger::getLogger(__CLASS__);

    $defaultOpts = array(
      'mongodb'    => null,         // MongoDB
      'name'       => 'default',    // collection name
      'bufferSize' => 100,          // buffer size
      'priority'   => 5,            // priority, 0 is most high priority
      'opid'       => uniqid('op'), // Operation ID
      'autoIndex'  => true,         // ensure indexes automatically
      'extraExpiredSec' => 60       // 失効延長時間(秒) 失効した場合 expiredWaitがコールされる.
    );

    $mergedOpts = array();
    foreach($defaultOpts as $k => $v) {
      $mergedOpts[$k] = isset($opts[$k]) ? $opts[$k]: $v;
      unset($opts[$k]);
    }

    if(!empty($opts))
      throw new InvalidArgumentException('Unknown opts keys : ' . implode(' and ',array_keys($opts)));

    if(!($mergedOpts['mongodb'] instanceof MongoDB))
      throw new InvalidArgumentException('mongodb of $opts is not MongoDB instance.');

    // integer
    foreach (array('bufferSize', 'priority', 'extraExpiredSec') as $k)
      if(!is_integer($mergedOpts[$k]))
        throw new InvalidArgumentException(sprintf('%s of $opts is only accept integer value.', $k));

    // string
    foreach (array('name', 'opid') as $k)
      if(!is_string($mergedOpts[$k]))
        throw new InvalidArgumentException(sprintf('%s of $opts is only accept string value.', $k));

    // bool
    foreach (array('autoIndex') as $k)
      if(!is_bool($mergedOpts[$k]))
        throw new InvalidArgumentException(sprintf('%s of $opts is only accept boolean value.', $k));

    $this->opts = $mergedOpts;
    $this->bufferSize = intval($mergedOpts['bufferSize']);
    $this->extraExpiredSec = intval($mergedOpts['extraExpiredSec']);
    $this->mcJobQueue = $mergedOpts['mongodb']->selectCollection(self::$prefixMC . $mergedOpts['name']);

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



  //public function set($func) {
  public function set($func) {
    if(!is_callable($func))
      throw new InvalidArgumentException('function only accepts function. Input was: ' . $func);
    $this->func = $func;
  }



  public function run() {
    $this->log->debug('call');
    $func = $this->func;
    if (!is_callable($func)) return false;
    $this->enableBuffer();
    try{
      while(!$this->terminated){
        $this->terminated = true;
        $func($this);
      }
    }catch(Exception $e){
      $this->disableBuffer();
      throw $e;
    }
    $this->disableBuffer();
  }



  public function isDone() {
    return 0 == $this->getCount();
  }



  /**
   * Producerが登録した処理中のJob数
   */
  public function getCount($opts=array()) {
    $cnt = $this->mcJobQueue->count(array('opid' => $this->opts['opid']));
    return $cnt;
  }



  public function enqueue($label, $value, $opts=array()){
    $defaultOpts = array(
      'priority' => $this->opts['priority']
    );

    $mergedOpts = array();
    foreach($defaultOpts as $k => $v) {
      $mergedOpts[$k] = isset($opts[$k]) ? $opts[$k]: $v;
      unset($opts[$k]);
    }

    if(!empty($opts))
      throw new InvalidArgumentException('Unknown opts keys : ' . implode(' and ',array_keys($opts)));

    // integer
    foreach (array('priority') as $k)
      if(!is_integer($mergedOpts[$k]))
        throw new InvalidArgumentException(sprintf('%s of $opts is only accept integer value.', $k));

    if(is_null($value))
      throw new InvalidArgumentException('value is not null.');

    if(!array_key_exists($label, $this->bufferDatas)) {
      $this->beforeCreateBuffer($label);
      $extraExpiredSec4Buffer = 1;
      $this->bufferDatas[$label] = array('jobs'=>array(), 'expired'=>time()+$extraExpiredSec4Buffer);
    }

    $this->bufferDatas[$label]['jobs'][] = array('value'=>$value, 'opts'=>$mergedOpts);

    if($this->log->isTraceEnabled()){
      $this->log->trace(sprintf('call enqueue label:%s, jobs:%d', $label, count($this->bufferDatas[$label]['jobs'])));
      //      $this->log->debug($this->bufferDatas);
    }

    $this->flush();
    $this->terminated = false;
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
    // ログで警告だけだすが処理は続行する. あとの対処はWorkerの処理を早くさせる等で解決させる.
    $this->log->warn(sprintf('expired wait, label %s of %s is still full.', $label, $this->getName()));
  }


  /**
   * 待ち時間を利用する目的で割り込みさせる場合このメソッドをオーバライド
   */
  protected function wait(){
    sleep(1);
  }



  protected function isBuffer(){
    return $this->buffered;
  }



  protected function enableBuffer(){
    $this->buffered=true;
  }



  protected function disableBuffer(){
    $this->buffered=false;
    $this->flush(true);
  }



  protected function flush($force=false){
    $force = $force === true;
    $flushedLabels = array();
    $tmpJobs = array();
    foreach($this->bufferDatas as $label => $datas){
      $jobs = $datas['jobs'];
      $cnt = count($jobs);
      if($cnt <= 0) continue;
      // force is true if buffer was expired
      if($datas['expired'] < time()){
        $force = true;
        if($this->log->isTraceEnabled()){
          $this->log->trace(sprintf('buffer datas expired, so force flush. label:%s ', $label));
        }
      }
      if(!$force and $this->isBuffer() and $cnt < $this->bufferSize) continue;
      foreach($jobs as $job) {
        $tmpJobs[] = array(
          'label'         => $label,
          'value'         => $job['value'],
          'priority'      => $job['opts' ]['priority'],
          'opid'          => $this->opts['opid'],
          'lockBy'        => null,
          'lockExpiredAt' => new MongoDate()// nowでロッックを即時解除
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



}

