<?php

namespace Test\ConcurrencyMongo\JobQueue;

use PHPUnit_Framework_TestCase;
use Mongo;
use MongoDB;
use Logger;
use ConcurrencyMongo\JobQueue\JobQueueProducer;
use ConcurrencyMongo\JobQueue\JobQueueWorker;
use ConcurrencyMongo\JobQueue\JobWorker;
use ConcurrencyMongo\JobQueue\Job;
use ConcurrencyMongo\JobQueue\ExpiredJobException;

class TestJobWorker implements JobWorker {

  private $func;

  function __construct($func) {
    $this->func = $func;
  }

  public function isWorkable($label) {
    return true;
  }

  public function assignJob($label, Job $job) {
    $func = $this->func;
    $func($job);
  }

}

class JobQueueWorkerTest extends PHPUnit_Framework_TestCase {

  public $log;
  public $mongoDB;
  public $producer;
  public $worker;

  /**
   * Sets up the fixture, for example, opens a network connection.
   * This method is called before a test is executed.
   */
  protected function setUp() {
    static $logName = null;
    if(null===$logName) $logName = str_replace('\\', '.', __CLASS__);
    $this->log = Logger::getLogger($logName);
    $this->log->info('call');

    $mongo = new Mongo();
    $this->mongoDB = $mongo->selectDB('test_php_cncurrencymongo');

    $this->producer = new JobQueueProducer(
      $this->mongoDB,
      array(
        'name'=>'test',
        'bufferSize'=>3,
        'extraExpiredSec'=>1
      )
    );

    $mcJobQueue = $this->mongoDB->selectCollection($this->producer->getName());
    $mcJobQueue->remove(array(), array('safe'=>true));

  }

  /**
   * Tears down the fixture, for example, closes a network connection.
   * This method is called after a test is executed.
   */
  protected function tearDown()
  {
    $this->log->info('tearDown');
  }



  /**
   * 正常系
   */
  public function testSimple(){
    $this->log->debug('call');
    $self = $this;

    // JobQueueにJobを登録
    $cnt = 0;
    $this->producer->set(function($q) use ($self, &$cnt){
      ++$cnt;
      if($cnt==1){
        $q->enqueue('LABEL_001', 'value 001_01', array('priority'=>1));
      }
    });
    $this->producer->run();

    // 
    $cnt = 0;
    $worker = new JobQueueWorker($this->mongoDB, array('name'=>'test'));
    $worker->addJobWorker('LABEL_001', new TestJobWorker(function($job) use ($self, &$cnt){
      ++$cnt;
      $self->log->trace(sprintf('cnt:%d value:%s', $cnt, $job->getValue()));

      if($cnt==1){
        // ジョブを解放してキャンセル
        $self->assertEquals('value 001_01', $job->getValue());
        $self->assertFalse($job->isExpired(),'Should not be expired');
        $job->release();
        $self->assertTrue($job->isExpired(),'Should be expired');
      }
      elseif($cnt==2){
        // ジョブを完了させる
        $self->assertEquals('value 001_01', $job->getValue());// キャンセルしたものが再取得される
        $self->assertFalse($job->isExpired(),'Should not be expired');
        $job->done();
        $self->assertTrue($job->isExpired(),'Should be expired');
      }
      else{
        $job->done();// doneさせないとデストラクタで自動的にreleaseされ無限ループになる
      }
    }));
    while($worker->run());// Jobがある限りループ

    $this->assertEquals(2, $cnt);
  }


  /**
   * 優先度
   */
  public function testPriority(){
    $this->log->info('call');
    $self = $this;

    $cnt = 0;
    $this->producer->set(function($q) use ($self, &$cnt){
      ++$cnt;
      if($cnt==1){
        $q->enqueue('LABEL_001', 'value 001_03', array('priority'=>3));
        $q->enqueue('LABEL_001', 'value 001_02', array('priority'=>2));
        $q->enqueue('LABEL_001', 'value 001_01', array('priority'=>1));
        $q->enqueue('LABEL_002', 'value 002_01', array('priority'=>1));
      }
    });
    $this->producer->run();

    $cnt = 0;
    $worker = new JobQueueWorker($this->mongoDB, array('name'=>'test'));
    $worker->addJobWorker('LABEL_001', new TestJobWorker(function($job) use ($self, &$cnt){
      ++$cnt;
      $self->log->debug($job->getValue());
      if($cnt==1) $self->assertEquals('value 001_01', $job->getValue());
      if($cnt==2) $self->assertEquals('value 001_02', $job->getValue());
      if($cnt==3) $self->assertEquals('value 001_03', $job->getValue());
      $job->done();
    }));
    while($worker->run());
    $self->assertEquals(3, $cnt);
  }



  /**
   * Jobの失効
   */
  public function testJobExpired(){
    $this->log->debug('call');
    $self = $this;

    $cnt = 0;
    $this->producer->set(function($q) use ($self, &$cnt){
      ++$cnt;
      if($cnt==1){
        $q->enqueue('LABEL_001', 'value 001_01', array('priority'=>1));
        $q->enqueue('LABEL_001', 'value 001_02', array('priority'=>2));
      }
    });
    $this->producer->run();

    $cnt = 0;
    $exceptions = array();
    $worker = new JobQueueWorker($this->mongoDB, array('name'=>'test', 'extraExpiredSec'=>2));
    $worker->addJobWorker('LABEL_001', new TestJobWorker(function($job) use ($self, &$cnt, &$exceptions){
      ++$cnt;
      $self->log->trace(sprintf('cnt:%d value:%s', $cnt, $job->getValue()));
      
      if($cnt==1){
        // 失効 (タイムアウト)
        $self->assertEquals('value 001_01', $job->getValue());
        sleep(3);
        $self->assertTrue($job->isExpired(),'Should be expired');
        try{
          $job->done();
          $self->fail('Shold be thrown an ExpiredJobException');
        }catch(ExpiredJobException $e){
          $self->log->trace('Caught ExpiredJobException');
          $exceptions[] = $e;
        }
      }
      elseif($cnt==2){
        // 失効 (プロパティ参照)
        $self->assertEquals('value 001_01', $job->getValue());// 失効したものが再取得される.
        $job->forceExpire();// 強制的に失効させる
        $self->assertTrue($job->isExpired(),'Should be expired');
        try{
          $job->getValue();
          $self->fail('Shold be thrown an ExpiredJobException');
        }catch(ExpiredJobException $e){
          $self->log->trace('Caught ExpiredJobException');
          $exceptions[] = $e;
        }
      }
      else{
        $job->done();
      }
    }));
    while($worker->run());

    $self->assertEquals(4, $cnt);// 失効2回の、実行中2で計4
    $self->assertCount(2, $exceptions);// 2回の失効例外
  }



}
