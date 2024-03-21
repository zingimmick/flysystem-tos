<?php

declare(strict_types=1);

namespace Zing\Flysystem\Tos;

class UnableToGetUrl extends \RuntimeException
{
    public static function missingOption(string $option): self
    {
        return new self(sprintf('Unable to get url with option %s missing.', $option));
    }
}
