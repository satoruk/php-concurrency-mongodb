<?php

namespace Test\ConcurrencyMongo\JobQueue;

use PHPUnit_Framework_TestCase;
use Mongo;
use Logger;
use ConcurrencyMongo\JobQueue\JobQueueProducer;

class JobQueueProducer4Test extends JobQueueProducer {

  public $cntBatchInsert = 0;
  public $cntJob = 0;
  public $cntExpiredWait = 0;



  protected function batchInsert($values){
    parent::batchInsert($values);
    $this->cntBatchInsert++;
    $len = count($values);
    if ($len > 0) $this->cntJob += $len;
  }



  protected function expiredWait($label){
    parent::expiredWait($label);
    $this->cntExpiredWait++;
  }



  protected function wait(){
    parent::wait();
    sleep(1);
  }
}



class JobQueueProducerTest extends PHPUnit_Framework_TestCase
{

  public $log;
  public $mongoDB;
  public $producer;



  /**
   * Sets up the fixture, for example, opens a network connection.
   * This method is called before a test is executed.
   */
  protected function setUp()
  {
    $this->log = Logger::getLogger('Test\ConcurrencyMongo\JobQueue');
    $this->log->info('setup');

    $mongo = new Mongo();
    $this->mongoDB = $mongo->selectDB('test_php_cncurrencymongo');

    $this->producer = new JobQueueProducer4Test(
      array(
        'mongodb'=>$this->mongoDB,
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
    $this->log->debug('tearDown');
  }



  /**
   * フラッシュタイミング(単一ラベル)
   */
  public function testFlushSingleLabel(){
    $this->log->debug('call');
    $self = $this;
    $cnt = 0;
    // enqueueルールを設定
    $this->producer->set(function($q) use ($self, &$cnt){
      ++$cnt;
      if($cnt==1){
        $q->enqueue('LABEL_001', 'value 001_01');
        $q->enqueue('LABEL_001', 'value 001_02');
        $q->enqueue('LABEL_001', 'value 001_03');
        // バッファサイズが3なのでこのタイミングでDBに書き込まれる.
        $self->log->info(sprintf('expected flush! batchInsert:%d, job:%d', $q->cntBatchInsert, $q->cntJob));
        $self->assertEquals(1, $q->cntBatchInsert, 'Should be 1 time batch insert');
        $self->assertEquals(3, $q->cntJob        , '3 jobs');
      } else {
        // enqueueされないと終了とみなされる.
        $self->log->info('done');
      }
    });
    $this->producer->run();
    $this->assertEquals(2, $cnt);
    $this->assertEquals(1, $this->producer->cntBatchInsert, 'Should be 1 time batch insert');
    $this->assertEquals(3, $this->producer->cntJob        , '3 jobs');
  }



  /**
   * フラッシュタイミング(複数ラベル)
   */
  public function testFlushEachLabel(){
    $this->log->info('call');
    $self = $this;
    $cnt = 0;
    $this->producer->set(function($q) use ($self, &$cnt){
      ++$cnt;
      if($cnt==1){
        $q->enqueue('LABEL_001a', 'value 001_01');
        $q->enqueue('LABEL_001b', 'value 001_11');
        $q->enqueue('LABEL_001b', 'value 001_12');
        $q->enqueue('LABEL_001b', 'value 001_13');// ラベル毎のバッファなのでここでflushされる.
        $self->log->info(sprintf('expectedflush!  batchInsert:%d, job:%d', $q->cntBatchInsert, $q->cntJob));
        $self->assertEquals(1, $q->cntBatchInsert, 'Should be 1 time batch insert');
        $self->assertEquals(3, $q->cntJob        , '3 jobs');
      }
      if($cnt==2){
        $q->enqueue('LABEL_002a', 'value 002_01');
        $self->assertEquals(1, $q->cntBatchInsert, 'Should be 1 time batch insert');
        $self->assertEquals(3, $q->cntJob        , '3 jobs');
      }
    });
    $this->producer->run();
    $this->assertEquals(3, $cnt);
    // 最後にまとめてflushされる
    $this->assertEquals(2, $this->producer->cntBatchInsert, 'Should be 2 time batch insert');
    $this->assertEquals(5, $this->producer->cntJob        , '5 jobs');
  }



  /**
   * バッファのタイムアウト処理
   */
  public function testBufferTImeout(){
    $this->log->info('call');
    $self = $this;
    $cnt = 0;
    $this->producer->set(function($q) use ($self, &$cnt){
      ++$cnt;
      if($cnt==1){
        $q->enqueue('LABEL_001', 'value 001_01');
        sleep(2);// 強制的にタイムアウト
        $self->assertEquals(0, $q->cntBatchInsert, 'Should be 0 times batch insert');
        $q->enqueue('LABEL_001', 'value 001_02');// timeoutでバッファがいったんflushされる
        $self->log->info(sprintf('expected flush!  batchInsert:%d, job:%d', $q->cntBatchInsert, $q->cntJob));
        $self->assertEquals(1, $q->cntBatchInsert, 'Should be 1 time batch insert');
        $self->assertEquals(2, $q->cntJob        , '2 jobs');
        $q->enqueue('LABEL_001', 'value 001_03');
      }
    });
    $this->producer->run();
    $this->assertEquals(2, $cnt);
    // 最後にまとめてflushされる
    $this->assertEquals(2, $this->producer->cntBatchInsert, 'Should be 2 times batch insert');
    $this->assertEquals(3, $this->producer->cntJob        , '3 jobs');
  }



  /**
   * JobQueue(label毎)が大きくなりすぎた場合の待ち
   */
  public function testWaitDuringJobQueueAsFull(){
    $this->log->info('call');
    $self = $this;
    $cnt = 0;
    $this->producer->set(function($q) use ($self, &$cnt){
      ++$cnt;
      if($cnt==1){
        $threshold = 30;// JobQueueがいっぱいになる閾値
        for ($i=1;$i<=$threshold;$i++) {
          $q->enqueue('LABEL_001', sprintf('value 001_%02d', $i));
        }
        $self->log->info(sprintf('expected flush!  batchInsert:%d, job:%d', $q->cntBatchInsert, $q->cntJob));
        $self->assertEquals(10, $q->cntBatchInsert, 'Should be 10 times batch insert');
        $self->assertEquals(30, $q->cntJob        , '30 jobs');
        $self->assertEquals(0, $q->cntExpiredWait, 'Should be 0 times expired wait');
        $q->enqueue('LABEL_001', sprintf('value 001_%02d', $i));
        $self->assertEquals(1, $q->cntExpiredWait, 'Should be 1 time expired wait');
        $self->assertEquals(10, $q->cntBatchInsert, 'Should be 10 times batch insert');
      }
    });
    $this->producer->run();
    $this->assertEquals(2, $cnt);
    $this->assertEquals(11, $this->producer->cntBatchInsert, 'Should be 11 times batch insert');
    $this->assertEquals(31, $this->producer->cntJob        , '31 jobs');
  }



  /**
   * Producerが登録した処理中のJob数の検証
   */
  public function testCount(){
    $this->log->info('call');
    $this->assertEquals(0, $this->producer->getCount());
    $this->producer->enqueue('LABEL_001', 'value 001_01');
    $this->assertEquals(1, $this->producer->getCount());
    $this->producer->enqueue('LABEL_001', 'value 001_02');
    $this->assertEquals(2, $this->producer->getCount());
  }


}
