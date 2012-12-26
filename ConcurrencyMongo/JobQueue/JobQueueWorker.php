<?php

namespace ConcurrencyMongo\JobQueue;

use InvalidArgumentException;
use MongoDB;
use Logger;
use ConcurrencyMongo\JobQueue\JobQueueException;
use ConcurrencyMongo\JobQueue\JobQueue;
use ConcurrencyMongo\JobQueue\Job;
use ConcurrencyMongo\JobQueue\JobWorker;

class JobQueueWorker
{

    private $log;
    protected $workers = array();
    protected $func;
    protected $jobQueue;



    public function __construct(MongoDB $mongoDB, $opts = array())
    {
        static $logName = null;
        if (null===$logName) {
            $logName = str_replace('\\', '.', __CLASS__);
        }
        $this->log = Logger::getLogger($logName);

        $defaultOpts = array(
            'opid' => uniqid('op'), // Operation ID
        );
        $defaultOpts = array_merge(JobQueue::$defaultOptions, $defaultOpts);

        $mergedOpts = array();
        foreach ($defaultOpts as $k => $v) {
            $mergedOpts[$k] = isset($opts[$k]) ? $opts[$k]: $v;
            unset($opts[$k]);
        }

        if (!empty($opts)) {
            throw new InvalidArgumentException('Unknown opts keys : ' . implode(' and ', array_keys($opts)));
        }

        // string
        foreach (array('opid') as $k) {
            if (!is_string($mergedOpts[$k])) {
                throw new InvalidArgumentException(sprintf('%s of $opts is only accept string value.', $k));
            }
        }

        $jqOpts = array();
        foreach (JobQueue::$defaultOptions as $k => $v) {
            $jqOpts[$k] = $mergedOpts[$k];
        }
        $this->jobQueue = new JobQueue($mongoDB, $jqOpts);

        $this->opid = $mergedOpts['opid'];
        // TODO MongoDB時間とLocal時間のズレを検証する
    }



    public function addJobWorker($label, JobWorker $worker)
    {
        static $errMsg = 'function only accepts \'%s\'. Input was \'%s\'';
        if (gettype($label)!=='string') {
            throw new InvalidArgumentException(sprintf($errMsg, 'string', gettype($label)));
        }
        if (is_null($worker)) {
            return false;
        }
        if (!in_array($label, $this->workers)) {
            $this->workers[$label] = array();
        }
        if (in_array($worker, $this->workers[$label], true)) {
            return false;
        }
        $this->workers[$label][] = $worker;
        return true;
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
    public function run()
    {
        if ($this->log->isTraceEnabled()) {
            $stacks = array();
            foreach (debug_backtrace() as $t) {
                $stacks[] = sprintf('file: %s on %d %s', @$t['file'], @$t['line'], @$t['function']);
            }
            $this->log->trace('call '.implode(PHP_EOL, $stacks));
        } else {
            $this->log->debug('call');
        }

        $labels = array();
        foreach (array_keys($this->workers) as $label) {
            if ($this->isWorkable($label)) {
                $labels[] = $label;
            }
        }

        if (empty($labels)) {
            return false;
        }

        $job = $this->jobQueue->findJob($this->opid, $labels);
        if (is_null($job)) {
            return false;
        }

        $label = $job->getLabel();
        $workers = $this->workers[$label];

        if ($this->log->isDebugEnabled()) {
            $this->log->debug(
                sprintf(
                    'JobQueueName:%s label:%s workers:%d',
                    $this->jobQueue->getName(),
                    $label,
                    count($workers)
                )
            );
        }

        foreach ($workers as $worker) {
            if ($worker->isWorkable($label)) {
                $worker->assignJob($label, $job);
            }
        }

        return true;
    }



    /**
     * ラベルのワーカーが実行できる場合true
     * 必要に応じてオーバライドして利用してください
     */
    public function isWorkable($label)
    {
        if (!array_key_exists($label, $this->workers)) {
            return false;
        }
        $workers = $this->workers[$label];
        foreach ($workers as $worker) {
            if ($worker->isWorkable($label)) {
                return true;
            }
        }
        return false;
    }
}
