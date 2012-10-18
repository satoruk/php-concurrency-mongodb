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
    $q->enqueue('opid_1', 'Mercury', 'v_2');
    $this->assertEquals(0, $q->countJob());
    $q->enqueue('opid_1', 'Sun'    , 'v_3');
    $this->assertEquals(2, $q->countJob());
    $this->assertEquals(2, $q->countLabel('Sun'));
    $q->enqueue('opid_2', 'Mercury', 'v_4');// Operation id(opid)は関係ない
    $this->assertEquals(4, $q->countJob());
    $this->assertEquals(2, $q->countLabel('Mercury'));
    $q->enqueue('opid_1', 'Venus'  , 'v_5');
    $this->assertEquals(4, $q->countJob());
    $q->disableBuffer();// バッファを無効にすると自動でflushされる.
    $this->assertEquals(5, $q->countJob());
  }



  /**
   * バッファは一定時間経つとフラッシュされる
   */
  public function testBufferByExpiredAt(){
    $this->log->info('call');
    $q = new JobQueue($this->mongoDB, array('name'=>'test', 'bufferSize'=>2));
    $q->enableBuffer();
    $this->assertEquals(0, $q->countJob());
    $q->enqueue('opid_1', 'Sun'    , 'v_1');
    sleep(2);// 1秒で超える
    $q->enqueue('opid_1', 'Mercury', 'v_2');// flush here
    $this->assertEquals(1, $q->countJob());
    $this->assertEquals(1, $q->countLabel('Sun'));
    $this->log->debug('バッファを無効にすると自動でflushされる.');
    $q->disableBuffer();
    $this->assertEquals(2, $q->countJob());
  }


}
