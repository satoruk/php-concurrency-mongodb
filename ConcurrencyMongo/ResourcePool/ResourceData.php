<?php

namespace ConcurrencyMongo\ResourcePool;

use ConcurrencyMongo\ResourcePool\ResourceException;
use ConcurrencyMongo\ResourcePool\ResourcePool;

class ResourceData
{

    protected static $messageAlreadyReleased = 'This resource is already released.';

    protected $released;
    protected $pool;
    protected $data;



    public function __construct($pool, &$data)
    {
        $this->pool = $pool;
        $this->data = &$data;
        $this->released = false;
    }



    public function __destruct()
    {
        if (!$this->isReleased()) {
            $this->release();
        }
    }



    public function getValue()
    {
        if ($this->isReleased()) {
            throw new ResourceException(self::$messageAlreadyReleased);
        }
        $this->update();
        return @$this->data['data']['resource'];
    }



    protected function update()
    {
        $this->pool->update();
    }



    public function release($blockSec = null)
    {
        if ($this->isReleased()) {
            throw new ResourceException(self::$messageAlreadyReleased);
        }
        if (is_null($blockSec)) {
            $blockSec = ResourcePool::$statusInFree;
        } else {
            $blockSec += time();
        }
        $this->data['status'] = $blockSec;
        $this->released = true;
        $this->pool->update();
    }



    public function isReleased()
    {
        return $this->released;
    }



    public function broken()
    {
        if ($this->released) {
            throw new ResourceException(self::$messageAlreadyReleased);
        }
        $this->release();
    }
}
