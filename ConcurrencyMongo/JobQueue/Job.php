<?php

namespace ConcurrencyMongo\JobQueue;

use Exception;
use InvalidArgumentException;
use MongoCollection;
use MongoDate;
use Logger;
use ConcurrencyMongo\JobQueue\ExpiredJobException;

class Job {

  private $log;

  protected $expired;
  protected $mcJobQueue;
  protected $opts;



  public function __construct($mcJobQueue, $opts=array()) {
    $this->log = Logger::getLogger(__CLASS__);

    $mergedOpts = array();
    $defaultOpts = array(
      '_id'           => null,
      'label'         => null,
      'value'         => null,
      'lockBy'        => null,
      'lockExpiredAt' => null,
      'opid'          => null,
      'priority'      => null
    );

    foreach($defaultOpts as $k => $v) {
      $mergedOpts[$k] = isset($opts[$k]) ? $opts[$k]: $v;
      unset($opts[$k]);
    }

    if(!empty($opts) )
      throw new InvalidArgumentException('Unknown opts keys : ' . implode(' and ',array_keys($opts)));

    if(!($mcJobQueue instanceof MongoCollection))
      throw new InvalidArgumentException('mcJobQueue of $opts is not MongoCollection instance.');

    $this->mcJobQueue = $mcJobQueue;
    $this->opts = $mergedOpts;
    $this->expired=false;
  }



  public function getLabel() {
    $this->validateExpired();
    return $this->opts['label'];
  }



  public function getValue() {
    $this->validateExpired();
    return $this->opts['value'];
  }



  public function release() {
    if($this->expired) return true;
    $this->log->debug('release');
    $criteria = array('_id' => $this->opts['_id'], 'lockBy' => $this->opts['lockBy'], 'lockExpiredAt' => $this->opts['lockExpiredAt']);
    $newdata  = array('$set' => array('lockBy' => null, 'lockExpiredAt' => new MongoDate(0)));
    $options  = array('safe' => true);
    $v = $this->mcJobQueue->update($criteria, $newdata, $options);
    if($v['ok'] != 1) throw new Exception('Mongo error : ' . var_export($v, true));// TODO Exceptionを継承する.
    if($v['n' ] != 1) throw new Exception('Should be update a job : ' . var_export($v, true));
    $this->expired = true;
  }



  public function done() {
    $this->validateExpired();
    $this->log->debug('done');
    $criteria = array('_id' => $this->opts['_id'], 'lockBy' => $this->opts['lockBy'], 'lockExpiredAt' => $this->opts['lockExpiredAt']);
    $options  = array('safe' => true);
    $v = $this->mcJobQueue->remove($criteria, $options);
    if($v['ok'] != 1) throw new Exception('Mongo error : ' . var_export($v, true));// TODO Exceptionを継承する.
    if($v['n' ] != 1) throw new Exception('Should be update a job : ' . var_export($v, true));
    $this->expired = true;
  }


  public function isExpired() {
    if($this->expired) return true;
    $this->log->debug('isExpired');
    // 失効を検証
    if($this->opts['lockExpiredAt']->sec <= time()){
      $this->release();
    }
    return $this->expired;
  }


  public function forceExpire() {
    if($this->isExpired()) return;
    $this->release();
  }


  protected function validateExpired() {
    if(!$this->isExpired()) return;
    $this->log->debug('Expired Job Exception');
    throw new ExpiredJobException();
  }

}

