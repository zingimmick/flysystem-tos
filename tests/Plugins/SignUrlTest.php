<?php

declare(strict_types=1);

namespace Zing\Flysystem\Tos\Tests\Plugins;

use League\Flysystem\Filesystem;
use Zing\Flysystem\Tos\Plugins\SignUrl;
use Zing\Flysystem\Tos\Tests\TestCase;
use Zing\Flysystem\Tos\TosAdapter;

/**
 * @internal
 */
final class SignUrlTest extends TestCase
{
    public function testSignUrl(): void
    {
        $adapter = \Mockery::mock(TosAdapter::class);
        $adapter->shouldReceive('signUrl')
            ->withArgs(['test', 10, [], 'GET'])->once()->andReturn('test-url');
        $filesystem = new Filesystem($adapter);
        $filesystem->addPlugin(new SignUrl());
        $this->assertSame('test-url', $filesystem->signUrl('test', 10));
    }
}
