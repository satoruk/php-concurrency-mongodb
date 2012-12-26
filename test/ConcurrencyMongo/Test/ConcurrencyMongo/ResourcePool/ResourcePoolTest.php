<?php

namespace ConcurrencyMongo\Test\ConcurrencyMongo\ResourcePool;

use PHPUnit_Framework_TestCase;
use Exception;
use Mongo;
use Logger;
use ConcurrencyMongo\ResourcePool\ExpiredResourceException;
use ConcurrencyMongo\ResourcePool\ResourcePool;

/**
 */
class ResourcePoolTest extends PHPUnit_Framework_TestCase
{


    public $log;
    public $mongoDB;



    protected function setUp()
    {
        static $logName = null;
        if (null===$logName) {
            $logName = str_replace('\\', '.', __CLASS__);
        }
        $this->log = Logger::getLogger($logName);
        $this->log->info('setup');

        $mongo = new Mongo();
        $this->mongoDB = $mongo->selectDB('test_php_cncurrencymongo');

        $pool = new ResourcePool($this->mongoDB, 'test');
        $pool->drop();
    }



    protected function tearDown()
    {
    }



    /**
     * 単一プールでのテストケース
     */
    public function testSingle()
    {
        $this->log->debug('call');

        $uuid = uniqid('t-', true);
        $pool = new ResourcePool($this->mongoDB, 'test', array('uuid' => $uuid));
        foreach (explode(' ', 'a b c d e') as $v) {
            $pool->def($v);
        }

        // 未割当のリソースの確認
        $this->assertTrue($pool->hasFree());

        $datas1 = $pool->getAll();
        $this->assertEquals(5, count($datas1));

        // 解放してないため、リソースなし
        $datas2 = $pool->getAll();
        $this->assertEquals(0, count($datas2));

        // 未割当のリソースの確認
        $this->assertFalse($pool->hasFree());

        // 全て解放して取得
        foreach ($datas1 as $data) {
            $data->release();
        }
        $datas3 = $pool->getAll();
        $this->assertEquals(5, count($datas3));

        // cのみを解放して、cを取得できるか？
        foreach ($datas3 as $data) {
            if ($data->getValue() != 'c') {
                continue;
            }
            $data->release();
        }
        $datas4 = $pool->getAll();
        $this->assertEquals(1, count($datas4), 'cのみを解放してcを取得');
        $this->assertEquals('c', $datas4[0]->getValue());
    }



    /**
     * 自動延長テスト
     */
    public function testAutomaticUpdateExpiredTime()
    {
        $this->log->debug('call');

        /*
         * 計算上では
         * 調整時間 [sec] 4sec * 0.5 = 2sec
         *       PHP上での更新ロック時間 [extraSecLock]  2sec = 2sec
         *          悲観的ロック更新時間 [extraSecIn  ]  6sec = 4sec + 2sec
         * MongoDB上での悲観的ロック時間 [extraSecEx  ]  8sec = 4sec + 2sec + 2sec
         */
        $pool = new ResourcePool($this->mongoDB, 'test', array('extraSec' => 3, 'extraSecRate' => 0.5));
        foreach (explode(' ', 'a b c d e') as $v) {
            $pool->def($v);
        }

        $data = $pool->get();
        $this->assertNotNull($data);
        $data->release();

        // 更新ロック内なので更新されない
        sleep(1);// 更新後 1 sec (total 1 sec)
        $data = $pool->get();
        $this->assertNotNull($data);
        $data->release();

        // 悲観的ロックの更新時間(このタイミングで自動延長される)
        sleep(3);// 更新後 4 sec (total 4 sec)
        $data = $pool->get();
        $this->assertNotNull($data);
        $data->release();

        // 自動延長されていれば引き続きリソースが取得できる.
        sleep(3);// 更新後 3 sec (total 7 sec)
        $data = $pool->get();
        $this->assertNotNull($data);
        $data->release();

        // ロックが解除されるまでSleep
        try {
            sleep(7);// 更新後 7 sec (total 14 sec)
            $data = $pool->get();
            $this->fail('Should be throw an Exception');
        } catch (ExpiredResourceException $e) {
            // 期限切れで例外が発生する.
            $this->assertInstanceOf('ConcurrencyMongo\ResourcePool\ExpiredResourceException', $e);
            //$this->assertRegExp('//', $e->getMessage());
            return;
        }
    }



    /**
     * 複数プールの連携テストケース(タイムスライシングを想定)
     */
    public function testCombination1()
    {
        $this->log->debug('call');

        $pool = new ResourcePool($this->mongoDB, 'test');
        foreach (explode(' ', 'a b c d e') as $v) {
            $pool->def($v);
        }

        $datas = $pool->getAll();
        $this->assertEquals(5, count($datas));
        unset($pool, $datas);// メモリから解放

        // デストラクト後、リソースが解放される.
        $pool = new ResourcePool($this->mongoDB, 'test');
        $datas = $pool->getAll();
        $this->assertEquals(5, count($datas));
    }



    /**
     * 複数プールの連携テストケース(内部更新ロック)
     */
    public function testCombination2()
    {
        $this->log->debug('call');

        // checkin時に内部更新ロックがかかる.
        $pool1 = new ResourcePool($this->mongoDB, 'test', array('uuid' => 'pool1'));
        $pool2 = new ResourcePool($this->mongoDB, 'test', array('uuid' => 'pool2'));

        // pool1はdefメソッドはロックを解除するため
        foreach (explode(' ', 'a b c d e') as $v) {
            $pool1->def($v);
        }

        $datas = $pool1->getAll();// ここで再ロックとリソースの割当が入る
        $this->assertEquals(2, count($datas));// リソースの割当が入ったので2個割り当てられる

        $datas = $pool2->getAll();// ロックされた状態なのでリソース割当が無い
        $this->assertEquals(0, count($datas));
    }



    /**
     * 複数プールの連携テストケース(内部更新ロック)
     */
    public function testCombination3()
    {
        $this->log->debug('call');

        // checkin時に内部更新ロックがかかる.
        $pool1 = new ResourcePool($this->mongoDB, 'test', array('uuid' => 'pool1'));

        // pool1はdefメソッドはロックを解除するため
        foreach (explode(' ', 'a b c d e') as $v) {
            $pool1->def($v);
        }

        // リソースが登録されたあとにプールを作る.
        $pool2 = new ResourcePool($this->mongoDB, 'test', array('uuid' => 'pool2'));

        $datas = $pool1->getAll();// ここで再ロックとリソースの割当が入る
        $this->assertEquals(2, count($datas));// リソースの割当が入ったので2個割り当てられる

        $datas = $pool2->getAll();
        $this->assertEquals(2, count($datas));
    }



    public function testNewArgument()
    {
        $this->log->debug('call');

        try {
            $pool = new ResourcePool($this->mongoDB, 'test', array('extraSec' => 2));
        } catch (Exception $e) {
            $this->fail('Should not be throw any Exception');
        }

        try {
            $pool = new ResourcePool($this->mongoDB, 'test', array('extraSecRate' => 0.9));
        } catch (Exception $e) {
            $this->fail('Should not be throw any Exception');
        }
    }



    public function testNewArgumentExceptionAtMongoDB()
    {
        $this->log->debug('call');

        $this->setExpectedException('PHPUnit_Framework_Error');
        $pool = new ResourcePool('', 'test');
    }



    public function testNewArgumentExceptionAtExtraSec1()
    {
        $this->log->debug('call');

        try {
            $pool = new ResourcePool($this->mongoDB, 'test', array('extraSec' => 0));
        } catch (Exception $e) {
            $this->assertInstanceOf('InvalidArgumentException', $e);
            $this->assertRegExp('/extraSec/', $e->getMessage());
            return;
        }
        $this->fail('Should be throw an InvalidArgumentException');
    }



    public function testNewArgumentExceptionAtExtraSec2()
    {
        $this->log->debug('call');

        try {
            $pool = new ResourcePool($this->mongoDB, 'test', array('extraSec' => 2.0));
        } catch (Exception $e) {
            $this->assertInstanceOf('InvalidArgumentException', $e);
            $this->assertRegExp('/extraSec/', $e->getMessage());
            return;
        }
        $this->fail('Should be throw an InvalidArgumentException');
    }



    public function testNewArgumentExceptionAtExtraSecRate1()
    {
        $this->log->debug('call');

        try {
            $pool = new ResourcePool($this->mongoDB, 'test', array('extraSecRate' => 0.0));
        } catch (Exception $e) {
            $this->assertInstanceOf('InvalidArgumentException', $e);
            $this->assertRegExp('/extraSecRate/', $e->getMessage());
            return;
        }
        $this->fail('Should be throw an InvalidArgumentException');
    }



    public function testNewArgumentExceptionAtExtraSecRate2()
    {
        $this->log->debug('call');

        try {
            $pool = new ResourcePool($this->mongoDB, 'test', array('extraSecRate' => 1.1));
        } catch (Exception $e) {
            $this->assertInstanceOf('InvalidArgumentException', $e);
            $this->assertRegExp('/extraSecRate/', $e->getMessage());
            return;
        }
        $this->fail('Should be throw an InvalidArgumentException');
    }



    public function testNewArgumentExceptionAtUnknownOpts()
    {
        $this->log->debug('call');

        // Unknown opts keys
        try {
            $pool = new ResourcePool($this->mongoDB, 'test', array('foo' => 1.0));
        } catch (Exception $e) {
            $this->assertInstanceOf('InvalidArgumentException', $e);
            $this->assertRegExp('/Unknown opts keys/', $e->getMessage());
            return;
        }
        $this->fail('Should be throw an InvalidArgumentException');
    }



    private function showResourcePool($pool)
    {
        $this->log->debug('call');

        $info = array();
        $info[] = 'uuid : '          . $pool->getUUID();
        $info[] = 'allcated size : ' . $pool->getAllocatedSize();
        echo 'ResourcePool ' . implode(', ', $info) . PHP_EOL;
    }
}
