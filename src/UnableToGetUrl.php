<?php

declare(strict_types=1);

namespace Zing\Flysystem\Tos;

use League\Flysystem\FilesystemException;

class UnableToGetUrl extends \RuntimeException implements FilesystemException
{
    public static function missingOption(string $option): self
    {
        return new self(sprintf('Unable to get url with option %s missing.', $option));
    }
}
