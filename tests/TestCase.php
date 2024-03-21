<?php

declare(strict_types=1);

namespace Zing\Flysystem\Tos\Tests;

use GuzzleHttp\Psr7\Utils;
use PHPUnit\Framework\TestCase as BaseTestCase;

use function GuzzleHttp\Psr7\stream_for;

abstract class TestCase extends BaseTestCase
{
    /**
     * @return \Psr\Http\Message\StreamInterface
     */
    protected function streamFor(string $resource = '', array $options = [])
    {
        if (\function_exists('\GuzzleHttp\Psr7\stream_for')) {
            return stream_for($resource, $options);
        }

        return Utils::streamFor($resource, $options);
    }
}
