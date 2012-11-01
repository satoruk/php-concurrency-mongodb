<?php

namespace ConcurrencyMongo\JobQueue;

use InvalidArgumentException;
use MongoDB;
use Logger;
use ConcurrencyMongo\JobQueue\JobQueueException;
use ConcurrencyMongo\JobQueue\JobQueue;
use ConcurrencyMongo\JobQueue\Job;


class JobQueueWorker {

  private $log;
  protected $workers = array();
  protected $func;
  protected $jobQueue;



  public function __construct(MongoDB $mongoDB, $opts=array()) {
    $this->log = Logger::getLogger(__CLASS__);

    $defaultOpts = array(
      'opid' => uniqid('op'), // Operation ID
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
    // TODO MongoDB時間とLocal時間のズレを検証する
  }



  public function add($label, $func) {
    if(!is_string($label))
      throw new InvalidArgumentException('function only accepts string. Input was: ' . $label);

    if(!is_callable($func))
      throw new InvalidArgumentException('function only accepts function. Input was: ' . $func);

    if(in_array($label, $this->workers))
      $this->workers[$label] = array();

    $this->workers[$label][] = $func;
  }


  /**
   *
   * $worker = new JobQueueWorker();
   * 
   * while($worker->run());// JobQueueに入っているJobが全てなくなるまでループ
   *
   * Jobを一つ処理する
   * 処理したらtrue
   */
  public function run() {
    $this->log->debug('call');

    // TODO: runメソッドが実行される度にLabelが変わるようにする.

    $labels = array();
    foreach(array_keys($this->workers) as $label){
      if($this->isWorkable($label)) $labels[] = $label;
    }

    if(empty($labels)) return false;

    $job = $this->jobQueue->findJob($this->opid, $labels);
    if(is_null($job)) return false;

    $label = $job->getLabel();
    $workers = $this->workers[$label];

    if($this->log->isDebugEnabled()){
      $this->log->debug(sprintf(
        'JobQueueName:%s label:%s workers:%d',
        $this->jobQueue->getName(),
        $label,
        count($workers)
      ));
    }

    foreach($workers as $worker){
      $worker($job);
    }

    return true;
  }



  /**
   * ラベルのワーカーが実行できる場合true
   * 必要に応じてオーバライドして利用してください
   */
  public function isWorkable($label){
    return array_key_exists($label, $this->workers);
  }



}


