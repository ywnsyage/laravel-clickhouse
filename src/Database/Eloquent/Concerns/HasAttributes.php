<?php

declare(strict_types=1);

namespace Ywnsyage\LaravelClickHouse\Database\Eloquent\Concerns;

use LogicException;
use Illuminate\Database\Eloquent\Relations\Relation as BaseRelation;
use Ywnsyage\LaravelClickHouse\Database\Eloquent\Relations\Relation;
use Illuminate\Database\Eloquent\Concerns\HasAttributes as BaseHasAttributes;

trait HasAttributes
{
    use BaseHasAttributes;

    public function getDates(): array
    {
        return $this->dates;
    }

    public function getCasts(): array
    {
        return $this->casts;
    }

    protected function getDateFormat(): string
    {
        return $this->dateFormat ?? 'Y-m-d H:i:s';
    }

    protected function getRelationshipFromMethod($method)
    {
        $relation = $this->$method();

        if (! ($relation instanceof Relation || $relation instanceof BaseRelation)) {
            throw new LogicException(sprintf(
                '%s::%s must return a relationship instance.', static::class, $method
            ));
        }

        return tap($relation->getResults(), function ($results) use ($method) {
            $this->setRelation($method, $results);
        });
    }
}
