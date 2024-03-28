<?php

declare(strict_types=1);

namespace Zing\Flysystem\Tos\Tests;

use GuzzleHttp\Psr7\Utils;
use PHPUnit\Framework\TestCase as BaseTestCase;
use Psr\Http\Message\StreamInterface;

use function GuzzleHttp\Psr7\stream_for;

abstract class TestCase extends BaseTestCase
{
    /**
     * @param array{size?: int, metadata?: array<string, mixed>} $options
     */
    protected function streamFor(string $content = '', array $options = []): StreamInterface
    {
        if (\function_exists('\GuzzleHttp\Psr7\stream_for')) {
            return stream_for($content, $options);
        }

        return Utils::streamFor($content, $options);
    }

    /**
     * @param array{size?: int, metadata?: array<string, mixed>} $options
     *
     * @return resource
     */
    protected function streamForResource(string $content = '', array $options = [])
    {
        /** @var resource $resource */
        $resource = $this->streamFor($content, $options)
            ->detach();

        return $resource;
    }
}
