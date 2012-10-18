<?php

namespace ConcurrencyMongo\JobQueue;

use Exception;
use InvalidArgumentException;
use MongoDB;
use MongoDate;
use Logger;
use ConcurrencyMongo\JobQueue\JobQueue;

class JobQueueProducer {

  private $log;

  protected $opid;
  protected $func;
  protected $terminated = false;
  protected $jobQueue;

  public function __construct(MongoDB $mongoDB, $opts=array()) {
    $this->log = Logger::getLogger(__CLASS__);

    $defaultOpts = array(
      'opid' => uniqid('op') // Operation ID
    );
    $defaultOpts = array_merge(JobQueue::$defaultOptions, $defaultOpts);

    $mergedOpts = array();
    foreach($defaultOpts as $k => $v) {
      $mergedOpts[$k] = isset($opts[$k]) ? $opts[$k]: $v;
      unset($opts[$k]);
    }

    if(!empty($opts))
      throw new InvalidArgumentException('Unknown opts keys : ' . implode(' and ',array_keys($opts)));

    // string
    foreach (array('opid') as $k)
      if(!is_string($mergedOpts[$k]))
        throw new InvalidArgumentException(sprintf('%s of $opts is only accept string value.', $k));

    $jqOpts = array();
    foreach(JobQueue::$defaultOptions as $k => $v){
      $jqOpts[$k] = $mergedOpts[$k];
    }
    $this->jobQueue = new JobQueue($mongoDB, $jqOpts);

    $this->opid = $mergedOpts['opid'];
  }



  public function getJobQueue(){
    return $this->jobQueue;
  }



  public function getName(){
    return $this->jobQueue->getName();
  }



  public function set($func) {
    if(!is_callable($func))
      throw new InvalidArgumentException('function only accepts function. Input was: ' . $func);
    $this->func = $func;
  }



  public function run() {
    $this->log->debug('call');
    $func = $this->func;
    if (!is_callable($func)) return false;
    $this->jobQueue->enableBuffer();
    try{
      while(!$this->terminated){
        $this->terminated = true;
        $func($this);
      }
    }catch(Exception $e){
      $this->jobQueue->disableBuffer();
      throw $e;
    }
    $this->jobQueue->disableBuffer();
  }



  public function isDone() {
    return 0 == $this->getCount($this->opid);
  }



  /**
   * Producerが登録した処理中のJob数
   */
  public function getCount() {
    $cnt = $this->jobQueue->countJob($this->opid);
    return $cnt;
  }



  public function enqueue($label, $value, $opts=array()){
    $this->jobQueue->enqueue($this->opid, $label, $value, $opts);
    $this->terminated = false;
  }



}

