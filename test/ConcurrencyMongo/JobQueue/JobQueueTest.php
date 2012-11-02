<?php

namespace Test\ConcurrencyMongo\JobQueue;

use PHPUnit_Framework_TestCase;
use Mongo;
use Logger;
use ConcurrencyMongo\JobQueue\JobQueue;



class JobQueueTest extends PHPUnit_Framework_TestCase
{

  public $log;
  public $mongoDB;



  /**
   * Sets up the fixture, for example, opens a network connection.
   * This method is called before a test is executed.
   */
  protected function setUp()
  {
    $this->log = Logger::getLogger(__CLASS__);
    $this->log->info('call');

    $mongo = new Mongo();
    $this->mongoDB = $mongo->selectDB('test_php_cncurrencymongo');

    $queue = new JobQueue($this->mongoDB, array('name'=>'test'));

    $mcJobQueue = $this->mongoDB->selectCollection($queue->getName());
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
   */
  public function testCount(){
    $this->log->info('call');
    $q = new JobQueue($this->mongoDB, array('name'=>'test'));
    $this->assertEquals(0, $q->countJob());
    $q->enqueue('opid_1', 'Sun'    , 'v_1');
    $q->enqueue('opid_1', 'Mercury', 'v_2');
    $q->enqueue('opid_2', 'Mercury', 'v_3');
    $this->assertEquals(3, $q->countJob());
    $this->assertEquals(2, $q->countJob('opid_1'));
    $this->assertEquals(1, $q->countLabel('Sun'));
    $this->assertEquals(0, $q->countLabel('Sun'    , 'opid_2'));
    $this->assertEquals(1, $q->countLabel('Mercury', 'opid_2'));
  }



  /**
   */
  public function testBufferBySingleLabel(){
    $this->log->info('call');
    $q = new JobQueue($this->mongoDB, array('name'=>'test', 'bufferSize'=>2));
    $q->enableBuffer();
    $this->assertEquals(0, $q->countJob());
    $q->enqueue('opid_1', 'Sun', 'v_1');
    $this->assertEquals(0, $q->countJob());
    $q->enqueue('opid_1', 'Sun', 'v_2');// expected flush
    $this->assertEquals(2, $q->countJob());
    $q->enqueue('opid_1', 'Sun', 'v_3');
    $this->assertEquals(2, $q->countJob());
    $q->disableBuffer();// バッファを無効にすると自動でflushされる.
    $this->assertEquals(3, $q->countJob());
  }



  /**
   * バッファはラベル毎にフラッシュされる
   */
  public function testBufferByMultiLabel(){
    $this->log->info('call');
    $q = new JobQueue($this->mongoDB, array('name'=>'test', 'bufferSize'=>2));
    $q->enableBuffer();
    $this->assertEquals(0, $q->countJob());

    $q->enqueue('opid_1', 'Sun'    , 'v_1');
    $q->enqueue('opid_1', 'Mercury', 'v_2');// labelが閾値(bufferSize=2)を超えると、全てflush
    $this->assertEquals(2, $q->countJob());
    $this->assertEquals(1, $q->countLabel('Sun'));
    $this->assertEquals(1, $q->countLabel('Mercury'));

    $q->enqueue('opid_1', 'Sun'    , 'v_3');
    $this->assertEquals(2, $q->countJob());
    $this->assertEquals(1, $q->countLabel('Sun'));
    $this->assertEquals(1, $q->countLabel('Mercury'));

    $q->enqueue('opid_2', 'Mercury', 'v_4');// Operation id(opid)は関係ない
    $this->assertEquals(4, $q->countJob());
    $this->assertEquals(2, $q->countLabel('Sun'));
    $this->assertEquals(2, $q->countLabel('Mercury'));

    $q->enqueue('opid_1', 'Venus'  , 'v_5');
    $this->assertEquals(4, $q->countJob());
    $this->assertEquals(2, $q->countLabel('Sun'));
    $this->assertEquals(2, $q->countLabel('Mercury'));

    $q->disableBuffer();// バッファを無効にすると自動でflushされる.
    $this->assertEquals(5, $q->countJob());
    $this->assertEquals(2, $q->countLabel('Sun'));
    $this->assertEquals(2, $q->countLabel('Mercury'));
    $this->assertEquals(1, $q->countLabel('Venus'));
  }



  /**
   * JobQueue(label毎)が大きくなりすぎた場合の待ち
   */
  public function testWaitDuringJobQueueAsFull(){
    $this->log->info('call');
    $threshold = 20;// JobQueueがいっぱいになる閾値
    $q = new JobQueue($this->mongoDB, array('name'=>'test', 'bufferSize'=>2, 'extraExpiredSec'=>1));
    $q->enableBuffer();
    $this->assertEquals(0, $q->countJob());
    for ($i=1; $i<=$threshold; $i++) {
      $q->enqueue('opid_1', 'Sun', sprintf('v_%2d', $i));
    }
    $this->assertEquals(20, $q->countJob());
    $q->enqueue('opid_1', 'Sun', 'v_21');
    $q->disableBuffer();// しばらく待ち状態になるが、最終的にはenqueueされる.
    $this->assertEquals(21, $q->countJob());
  }



  /**
   */
  public function testFindJob(){
    $this->log->info('call');
    $q = new JobQueue($this->mongoDB, array('name'=>'test'));

    // Jobの登録
    $this->assertEquals(0, $q->countJob());
    $q->enqueue('opid_1', 'Sun', 'v_1');
    $this->assertEquals(1, $q->countJob());

    // jobの取得
    $job1 = $q->findJob('lockopid_1', array('Sun'));
    $this->assertNotNull($job1);
    $this->assertEquals('Sun', $job1->getLabel());
    $this->assertEquals('v_1', $job1->getValue());

    // jobは割当中なので取得できない
    $job2 = $q->findJob('lockopid_1', array('Sun'));
    $this->assertNull($job2);

    // jobを解放して再取得
    $job1->release();
    $job3 = $q->findJob('lockopid_1', array('Sun'));
    $this->assertEquals('Sun', $job3->getLabel());
    $this->assertEquals('v_1', $job3->getValue());

    // jobを完了させる
    $job3->done();
    $job4 = $q->findJob('lockopid_1', array('Sun'));
    $this->assertNull($job4);
  }



}
