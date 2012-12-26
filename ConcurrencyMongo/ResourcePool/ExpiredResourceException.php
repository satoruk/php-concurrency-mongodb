<?php

namespace ConcurrencyMongo\ResourcePool;

use ConcurrencyMongo\ResourcePool\ResourceException;

class ExpiredResourceException extends ResourceException
{
    public function __construct($message, $code = 0, Exception $previous = null)
    {
        parent::__construct($message, $code, $previous);
    }
}
