<?php

namespace ConcurrencyMongo\JobQueue;

use Exception;

class JobQueueException extends Exception
{

    public function __construct($message = 'job queue exception', $code = 0, Exception $previous = null)
    {
        parent::__construct($message, $code, $previous);
    }
}
