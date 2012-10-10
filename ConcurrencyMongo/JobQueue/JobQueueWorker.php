<?php

namespace ConcurrencyMongo\JobQueue;

use MongoDB;
use Exception;
use InvalidArgumentException;
use ConcurrencyMongo\JobQueue\Job;
use Logger;


class JobQueueWorker {

  protected static $prefixMC = 'JobQueue_';

  private $log;
  protected $workers = array();
  protected $opts;
  protected $func;
  protected $mdb;
  protected $mcJobQueue;



  public function __construct($opts=array()) {
    $this->log = Logger::getLogger(__CLASS__);

    $defaultOpts = array(
      'mongodb'    => null,         // MongoDB
      'name'       => 'anonymouse', // collection name
      'uuid'       => uniqid('op'), // Operation ID
      'extraSec'   => 3             //
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
    foreach (array('extraSec') as $k)
      if(!is_integer($mergedOpts[$k]))
        throw new InvalidArgumentException(sprintf('%s of $opts is only accept integer value.', $k));

    // string
    foreach (array('name','uuid') as $k)
      if(!is_string($mergedOpts[$k]))
        throw new InvalidArgumentException(sprintf('%s of $opts is only accept string value.', $k));

    // bool
    foreach (array() as $k)
      if(!is_bool($mergedOpts[$k]))
        throw new InvalidArgumentException(sprintf('%s of $opts is only accept boolean value.', $k));

    $this->opts = $mergedOpts;
    $this->mdb = $mergedOpts['mongodb'];
    $this->mcJobQueue = $mergedOpts['mongodb']->selectCollection(self::$prefixMC . $mergedOpts['name']);

    $this->log->trace($this->opts);

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
    $this->log->trace('call run');
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
    foreach($this->workers as $label => $workers){
      if($this->log->isDebugEnabled()){
        $this->log->debug(sprintf(
          'JobQueueName:%s label:%s workers:%d',
          $this->mcJobQueue->getName(),
          $label,
          count($workers)
        ));
      }
      $args = array(
        $this->mcJobQueue->getName(),
        $label,
        $this->opts['extraSec'] * 1000,// ミリ秒に変換
        $this->opts['uuid']
      );
      $v = $this->mdb->execute($code, $args);
      if(@$v['ok'] !== 1.0) throw new Exception('Mongo error : ' . var_export($v, true));// TODO Exceptionを継承する.
      if(is_null($v['retval'])) continue;
      $job = new Job($this->mcJobQueue, $v['retval']);
      foreach($workers as $worker){
        $worker($job);
      }
      return true;
    }
    return false;
  }

}


