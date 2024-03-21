<?php

declare(strict_types=1);

namespace Zing\Flysystem\Tos\Tests\Plugins;

use League\Flysystem\Filesystem;
use Tos\TosClient;
use Zing\Flysystem\Tos\Plugins\Kernel;
use Zing\Flysystem\Tos\Tests\TestCase;
use Zing\Flysystem\Tos\TosAdapter;

/**
 * @internal
 */
final class KernelTest extends TestCase
{
    public function testKernel(): void
    {
        $adapter = \Mockery::mock(TosAdapter::class);
        $adapter->shouldReceive('getClient')
            ->withNoArgs()
            ->once()
            ->andReturn(\Mockery::mock(TosClient::class));
        $filesystem = new Filesystem($adapter);
        $filesystem->addPlugin(new Kernel());
        $this->assertInstanceOf(TosClient::class, $filesystem->kernel());
    }
}
