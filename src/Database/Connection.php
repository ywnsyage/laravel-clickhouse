<?php

declare(strict_types=1);

namespace Ywnsyage\LaravelClickHouse\Database;

use Ywnsyage\ClickhouseBuilder\Query\Grammar;
use Ywnsyage\LaravelClickHouse\Database\Query\Builder;

class Connection extends \Ywnsyage\ClickhouseBuilder\Integrations\Laravel\Connection
{
    public function query()
    {
        return new Builder($this, new Grammar());
    }
}
