<?php

declare(strict_types=1);

namespace Zing\Flysystem\Tos\Tests;

use League\Flysystem\AdapterInterface;
use League\Flysystem\Config;
use League\Flysystem\Filesystem;
use Tos\Exception\TosClientException;
use Tos\Exception\TosServerException;
use Tos\TosClient;
use Zing\Flysystem\Tos\Plugins\FileUrl;
use Zing\Flysystem\Tos\Plugins\TemporaryUrl;
use Zing\Flysystem\Tos\TosAdapter;

/**
 * @internal
 */
final class InvalidAdapterTest extends TestCase
{
    /**
     * @var array<string, string>
     */
    private const CONFIG = [
        'key' => 'aW52YWxpZC1rZXk=',
        'secret' => 'aW52YWxpZC1zZWNyZXQ=',
        'bucket' => 'test',
        'endpoint' => 'tos-cn-shanghai.volces.com',
        'path_style' => '',
        'region' => '',
    ];

    /**
     * @var \Zing\Flysystem\Tos\TosAdapter
     */
    private $tosAdapter;

    /**
     * @var \TOS\TosClient
     */
    private $tosClient;

    protected function setUp(): void
    {
        parent::setUp();

        $this->tosClient = new TosClient(
            'cn-shanghai',
            self::CONFIG['key'],
            self::CONFIG['secret'],
            self::CONFIG['endpoint']
        );
        $this->tosAdapter = new TosAdapter($this->tosClient, self::CONFIG['bucket'], '', [
            'endpoint' => self::CONFIG['endpoint'],
        ]);
    }

    public function testUpdate(): void
    {
        $this->assertFalse($this->tosAdapter->update('file.txt', 'test', new Config()));
    }

    public function testUpdateStream(): void
    {
        $this->assertFalse(
            $this->tosAdapter->updateStream('file.txt', $this->streamFor('test')->detach(), new Config())
        );
    }

    public function testCopy(): void
    {
        $this->assertFalse($this->tosAdapter->copy('file.txt', 'copy.txt'));
    }

    public function testCreateDir(): void
    {
        $this->assertFalse($this->tosAdapter->createDir('path', new Config()));
    }

    public function testSetVisibility(): void
    {
        $this->assertFalse($this->tosAdapter->setVisibility('file.txt', AdapterInterface::VISIBILITY_PUBLIC));
    }

    public function testRename(): void
    {
        $this->assertFalse($this->tosAdapter->rename('from.txt', 'to.txt'));
    }

    public function testDeleteDir(): void
    {
        $this->expectException(TosServerException::class);
        $this->assertFalse($this->tosAdapter->deleteDir('path'));
    }

    public function testWriteStream(): void
    {
        $this->assertFalse(
            $this->tosAdapter->writeStream('file.txt', $this->streamFor('test')->detach(), new Config())
        );
    }

    public function testDelete(): void
    {
        $this->assertFalse($this->tosAdapter->delete('file.txt'));
    }

    public function testWrite(): void
    {
        $this->assertFalse($this->tosAdapter->write('file.txt', 'test', new Config()));
    }

    public function testRead(): void
    {
        $this->assertFalse($this->tosAdapter->read('file.txt'));
    }

    public function testReadStream(): void
    {
        $this->assertFalse($this->tosAdapter->readStream('file.txt'));
    }

    public function testGetVisibility(): void
    {
        $this->assertFalse($this->tosAdapter->getVisibility('file.txt'));
    }

    public function testGetMetadata(): void
    {
        $this->assertFalse($this->tosAdapter->getMetadata('file.txt'));
    }

    public function testListContents(): void
    {
        $this->expectException(TosServerException::class);
        $this->assertEmpty($this->tosAdapter->listContents());
    }

    public function testGetSize(): void
    {
        $this->assertFalse($this->tosAdapter->getSize('file.txt'));
    }

    public function testGetTimestamp(): void
    {
        $this->assertFalse($this->tosAdapter->getTimestamp('file.txt'));
    }

    public function testGetMimetype(): void
    {
        $this->assertFalse($this->tosAdapter->getMimetype('file.txt'));
    }

    public function testHas(): void
    {
        $this->assertFalse($this->tosAdapter->has('file.txt'));
    }

    public function testGetUrl(): void
    {
        $this->assertSame('https://test.tos-cn-shanghai.volces.com/file.txt', $this->tosAdapter->getUrl('file.txt'));
    }

    public function testSignUrl(): void
    {
        $this->expectException(TosClientException::class);
        $this->tosAdapter->setBucket('ab');
        $this->assertFalse($this->tosAdapter->signUrl('file.txt', 10, []));
    }

    public function testGetTemporaryUrl(): void
    {
        $this->expectException(TosClientException::class);
        $this->tosAdapter->setBucket('ab');
        $this->assertFalse($this->tosAdapter->getTemporaryUrl('file.txt', 10, []));
    }

    public function testSetBucket(): void
    {
        $this->assertSame('test', $this->tosAdapter->getBucket());
        $this->tosAdapter->setBucket('bucket');
        $this->assertSame('bucket', $this->tosAdapter->getBucket());
    }

    public function testGetClient(): void
    {
        $this->assertInstanceOf(TosClient::class, $this->tosAdapter->getClient());
    }

    public function testGetUrlWithUrl(): void
    {
        $client = \Mockery::mock(TosClient::class);
        $tosAdapter = new TosAdapter($client, '', '', [
            'endpoint' => '',
            'url' => 'https://tos.cdn.com',
        ]);
        $filesystem = new Filesystem($tosAdapter);
        $filesystem->addPlugin(new FileUrl());
        $this->assertSame('https://tos.cdn.com/test', $filesystem->getUrl('test'));
    }

    public function testGetUrlWithBucketEndpoint(): void
    {
        $client = \Mockery::mock(TosClient::class);
        $tosAdapter = new TosAdapter($client, '', '', [
            'endpoint' => 'https://tos.cdn.com',
            'bucket_endpoint' => true,
        ]);
        $filesystem = new Filesystem($tosAdapter);
        $filesystem->addPlugin(new FileUrl());
        $this->assertSame('https://tos.cdn.com/test', $filesystem->getUrl('test'));
    }

    public function testGetTemporaryUrlWithUrl(): void
    {
        $tosAdapter = new TosAdapter($this->tosClient, 'test', '', [
            'endpoint' => 'https://tos.cdn.com',
            'temporary_url' => 'https://tos.cdn.com',
        ]);
        $filesystem = new Filesystem($tosAdapter);
        $filesystem->addPlugin(new TemporaryUrl());
        $this->assertStringStartsWith('https://tos.cdn.com/test', (string) $filesystem->getTemporaryUrl('test', 10));
    }
}
