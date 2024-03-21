<?php

declare(strict_types=1);

namespace Zing\Flysystem\Tos\Tests\Plugins;

use League\Flysystem\Filesystem;
use Zing\Flysystem\Tos\Plugins\TemporaryUrl;
use Zing\Flysystem\Tos\Tests\TestCase;
use Zing\Flysystem\Tos\TosAdapter;

/**
 * @internal
 */
final class TemporaryUrlTest extends TestCase
{
    public function testGetTemporaryUrl(): void
    {
        $adapter = \Mockery::mock(TosAdapter::class);
        $adapter->shouldReceive('getTemporaryUrl')
            ->withArgs(['test', 10, [], 'GET'])->once()->andReturn('test-url');
        $filesystem = new Filesystem($adapter);
        $filesystem->addPlugin(new TemporaryUrl());
        $this->assertSame('test-url', $filesystem->getTemporaryUrl('test', 10));
    }
}
