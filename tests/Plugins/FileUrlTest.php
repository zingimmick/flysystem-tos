<?php

declare(strict_types=1);

namespace Zing\Flysystem\Tos\Tests\Plugins;

use League\Flysystem\Filesystem;
use Zing\Flysystem\Tos\Plugins\FileUrl;
use Zing\Flysystem\Tos\Tests\TestCase;
use Zing\Flysystem\Tos\TosAdapter;

/**
 * @internal
 */
final class FileUrlTest extends TestCase
{
    public function testGetUrl(): void
    {
        $adapter = \Mockery::mock(TosAdapter::class);
        $adapter->shouldReceive('getUrl')
            ->withArgs(['test'])->once()->andReturn('test-url');
        $filesystem = new Filesystem($adapter);
        $filesystem->addPlugin(new FileUrl());
        $this->assertSame('test-url', $filesystem->getUrl('test'));
    }
}
