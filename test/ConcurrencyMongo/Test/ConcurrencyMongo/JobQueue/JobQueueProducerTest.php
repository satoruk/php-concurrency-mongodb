<?php

namespace ConcurrencyMongo\Test\ConcurrencyMongo\JobQueue;

use PHPUnit_Framework_TestCase;
use Mongo;
use Logger;
use ConcurrencyMongo\JobQueue\JobQueueProducer;

class JobQueueProducerTest extends PHPUnit_Framework_TestCase
{

    public $log;
    public $mongoDB;
    public $producer;
    public $jobQueue;



    protected function setUp()
    {
        static $logName = null;
        if (null===$logName) {
            $logName = str_replace('\\', '.', __CLASS__);
        }
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
        $this->jobQueue = $this->producer->getJobQueue();

        $mcJobQueue = $this->mongoDB->selectCollection($this->producer->getName());
        $mcJobQueue->remove(array(), array('safe'=>true));
    }



    protected function tearDown()
    {
        $this->log->debug('tearDown');
    }



    /**
     * フラッシュタイミング(単一ラベル)
     */
    public function testFlushSingleLabel()
    {
        $this->log->debug('call');
        $self = $this;
        $cnt = 0;
        // enqueueルールを設定
        $this->producer->set(
            function ($q) use ($self, &$cnt) {
                ++$cnt;
                if ($cnt==1) {
                    $q->enqueue('Sun', 'v_1');
                    $q->enqueue('Sun', 'v_2');
                    $self->assertEquals(0, $self->jobQueue->countJob());
                    $q->enqueue('Sun', 'v_3');
                    // バッファサイズが3なのでこのタイミングでDBに書き込まれる.
                    $self->assertEquals(3, $self->jobQueue->countJob());
                } else {
                    // enqueueされないと終了とみなされる.
                    $self->log->info('done');
                }
            }
        );
        $this->producer->run();
        $this->assertEquals(2, $cnt);
        $this->assertEquals(3, $this->jobQueue->countJob());
    }



    /**
     * フラッシュタイミング(複数ラベル)
     */
    public function testFlushEachLabel()
    {
        $this->log->info('call');
        $self = $this;
        $cnt = 0;
        $this->producer->set(
            function ($q) use ($self, &$cnt) {
                ++$cnt;
                if ($cnt==1) {
                    $q->enqueue('Sun', 'v_1');
                    $q->enqueue('Mercury', 'v_2');
                    $q->enqueue('Mercury', 'v_3');// flush
                    $self->assertEquals(3, $self->jobQueue->countJob());
                    $q->enqueue('Mercury', 'v_4');
                    $self->assertEquals(3, $self->jobQueue->countJob());
                }
                if ($cnt==2) {
                    $self->assertEquals(3, $self->jobQueue->countJob());
                    $q->enqueue('Venus', 'v_5');
                    $self->assertEquals(3, $self->jobQueue->countJob());
                }
            }
        );
        $this->producer->run();
        $this->assertEquals(3, $cnt);
        // 最後にまとめてflushされる
        $this->assertEquals(5, $this->jobQueue->countJob());
        $this->assertEquals(1, $this->jobQueue->countLabel('Sun'));
        $this->assertEquals(3, $this->jobQueue->countLabel('Mercury'));
        $this->assertEquals(1, $this->jobQueue->countLabel('Venus'));
    }



    /**
     * Producerが登録した処理中のJob数の検証
     */
    public function testCount()
    {
        $this->log->info('call');
        $this->assertEquals(0, $this->jobQueue->countJob());
        $this->producer->enqueue('Sun', 'v_1');
        $this->assertEquals(1, $this->jobQueue->countJob());
        $this->producer->enqueue('Sun', 'v_2');
        $this->assertEquals(2, $this->jobQueue->countJob());
    }
}
