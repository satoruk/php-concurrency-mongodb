<?php

namespace ConcurrencyMongo\JobQueue;

use InvalidArgumentException;
use MongoCollection;
use MongoDate;
use Logger;
use ConcurrencyMongo\JobQueue\ExpiredJobException;
use ConcurrencyMongo\JobQueue\JobQueueException;

class Job
{

    private $log;

    protected $expired;
    protected $mcJobQueue;
    protected $opts;



    public function __construct(MongoCollection $mcJobQueue, $opts = array())
    {
        static $logName = null;
        if (null===$logName) {
            $logName = str_replace('\\', '.', __CLASS__);
        }
        $this->log = Logger::getLogger($logName);
        static $defaultOpts = array(
            '_id'           => null,
            'label'         => null,
            'value'         => null,
            'lockBy'        => null,
            'lockExpiredAt' => null,
            'opid'          => null,
            'priority'      => null,
            'resolution'    => null
        );
        $mergedOpts = array();

        foreach ($defaultOpts as $k => $v) {
            $mergedOpts[$k] = isset($opts[$k]) ? $opts[$k]: $v;
            unset($opts[$k]);
        }

        if (!empty($opts) ) {
            throw new InvalidArgumentException('Unknown opts keys : ' . implode(' and ', array_keys($opts)));
        }

        $this->mcJobQueue = $mcJobQueue;
        $this->opts = $mergedOpts;
        $this->expired=false;
    }



    public function __destruct()
    {
        $this->release();
    }



    public function getLabel()
    {
        $this->validateExpired();
        return $this->opts['label'];
    }



    public function getValue()
    {
        $this->validateExpired();
        return $this->opts['value'];
    }



    public function release()
    {
        if ($this->expired) {
            return true;
        }
        $this->log->debug('release');
        $criteria = array(
            '_id' => $this->opts['_id'],
            'lockBy' => $this->opts['lockBy'],
            'lockExpiredAt' => $this->opts['lockExpiredAt']
        );
        $newdata  = array('$set' => array('lockBy' => null, 'lockExpiredAt' => new MongoDate(0)));
        $options  = array('safe' => true);
        $v = $this->mcJobQueue->update($criteria, $newdata, $options);
        if ($v['ok'] != 1) {
            throw new JobQueueException('Mongo error : ' . var_export($v, true));
        }
        if ($v['n' ] >= 2) {
            throw new JobQueueException('Should be update a job : ' . var_export($v, true));
        }
        // n=0 既に失効してJobが他に割り当てられた, n=1 Jobを解放した.
        $this->expired = true;
    }



    public function done()
    {
        $this->validateExpired();
        $this->log->debug('done');
        $criteria = array(
            '_id' => $this->opts['_id'],
            'lockBy' => $this->opts['lockBy'],
            'lockExpiredAt' => $this->opts['lockExpiredAt']
        );
        $options  = array('safe' => true);
        $v = $this->mcJobQueue->remove($criteria, $options);
        if ($v['ok'] != 1) {
            throw new JobQueueException('Mongo error : ' . var_export($v, true));
        }
        if ($v['n' ] != 1) {
            throw new JobQueueException('Should be update a job : ' . var_export($v, true));
        }
        $this->expired = true;
    }


    public function isExpired()
    {
        if ($this->expired) {
            return true;
        }
        $this->log->debug('isExpired');
        // 失効を検証
        if ($this->opts['lockExpiredAt']->sec <= time()) {
            $this->release();
        }
        return $this->expired;
    }


    public function forceExpire()
    {
        if ($this->isExpired()) {
            return;
        }
        $this->release();
    }


    protected function validateExpired()
    {
        if (!$this->isExpired()) {
            return;
        }
        $this->log->debug('Expired Job Exception');
        throw new ExpiredJobException();
    }
}
