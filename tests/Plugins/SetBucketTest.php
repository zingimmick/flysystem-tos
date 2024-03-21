<?php

declare(strict_types=1);

namespace Zing\Flysystem\Tos\Tests\Plugins;

use League\Flysystem\Filesystem;
use Zing\Flysystem\Tos\Plugins\SetBucket;
use Zing\Flysystem\Tos\Tests\TestCase;
use Zing\Flysystem\Tos\TosAdapter;

/**
 * @internal
 */
final class SetBucketTest extends TestCase
{
    public function testSetBucket(): void
    {
        $adapter = \Mockery::mock(TosAdapter::class);
        $adapter->shouldReceive('setBucket')
            ->withArgs(['test'])->once()->passthru();
        $adapter->shouldReceive('getBucket')
            ->withNoArgs()
            ->once()
            ->passthru();
        $filesystem = new Filesystem($adapter);
        $filesystem->addPlugin(new SetBucket());
        $filesystem->bucket('test');
        $this->assertSame('test', $adapter->getBucket());
    }
}
