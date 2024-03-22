<?php

declare(strict_types=1);

namespace Zing\Flysystem\Tos\Tests;

use League\Flysystem\Config;
use League\Flysystem\UnableToCheckDirectoryExistence;
use League\Flysystem\UnableToCopyFile;
use League\Flysystem\UnableToCreateDirectory;
use League\Flysystem\UnableToDeleteFile;
use League\Flysystem\UnableToMoveFile;
use League\Flysystem\UnableToReadFile;
use League\Flysystem\UnableToRetrieveMetadata;
use League\Flysystem\UnableToSetVisibility;
use League\Flysystem\UnableToWriteFile;
use League\Flysystem\Visibility;
use Tos\TosClient;
use Zing\Flysystem\Tos\TosAdapter;
use Zing\Flysystem\Tos\UnableToGetUrl;

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

    private TosAdapter $tosAdapter;

    private TosClient $tosClient;

    protected function setUp(): void
    {
        parent::setUp();

        $this->tosClient = new TosClient(
            'cn-shanghai',
            self::CONFIG['key'],
            self::CONFIG['secret'],
            self::CONFIG['endpoint']
        );
        $this->tosAdapter = new TosAdapter($this->tosClient, self::CONFIG['bucket'], '');
    }

    public function testCopy(): void
    {
        $this->expectException(UnableToCopyFile::class);
        $this->tosAdapter->copy('file.txt', 'copy.txt', new Config());
    }

    public function testCreateDir(): void
    {
        $this->expectException(UnableToCreateDirectory::class);
        $this->tosAdapter->createDirectory('path', new Config());
    }

    public function testSetVisibility(): void
    {
        $this->expectException(UnableToSetVisibility::class);
        $this->tosAdapter->setVisibility('file.txt', Visibility::PUBLIC);
    }

    public function testRename(): void
    {
        $this->expectException(UnableToMoveFile::class);
        $this->tosAdapter->move('from.txt', 'to.txt', new Config());
    }

    public function testDeleteDir(): void
    {
        $this->expectException(\Tos\Exception\TosServerException::class);
        $this->tosAdapter->deleteDirectory('path');
    }

    public function testWriteStream(): void
    {
        $this->expectException(UnableToWriteFile::class);
        $this->tosAdapter->writeStream('file.txt', $this->streamForResource('test'), new Config());
    }

    public function testDelete(): void
    {
        $this->expectException(UnableToDeleteFile::class);
        $this->tosAdapter->delete('file.txt');
    }

    public function testWrite(): void
    {
        $this->expectException(UnableToWriteFile::class);
        $this->tosAdapter->write('file.txt', 'test', new Config());
    }

    public function testRead(): void
    {
        $this->expectException(UnableToReadFile::class);
        $this->tosAdapter->read('file.txt');
    }

    public function testReadStream(): void
    {
        $this->expectException(UnableToReadFile::class);
        $this->tosAdapter->readStream('file.txt');
    }

    public function testGetVisibility(): void
    {
        $this->expectException(UnableToRetrieveMetadata::class);
        $this->tosAdapter->visibility('file.txt')
            ->visibility();
    }

    public function testListContents(): void
    {
        $this->expectException(\Tos\Exception\TosServerException::class);
        $this->assertEmpty(iterator_to_array($this->tosAdapter->listContents('/', false)));
    }

    public function testGetSize(): void
    {
        $this->expectException(UnableToRetrieveMetadata::class);
        $this->tosAdapter->fileSize('file.txt')
            ->fileSize();
    }

    public function testGetTimestamp(): void
    {
        $this->expectException(UnableToRetrieveMetadata::class);
        $this->tosAdapter->lastModified('file.txt')
            ->lastModified();
    }

    public function testGetMimetype(): void
    {
        $this->expectException(UnableToRetrieveMetadata::class);
        $this->tosAdapter->mimeType('file.txt')
            ->mimeType();
    }

    public function testHas(): void
    {
        $this->assertFalse($this->tosAdapter->fileExists('file.txt'));
    }

    public function testBucket(): void
    {
        $tosAdapter = new TosAdapter($this->tosClient, self::CONFIG['bucket'], '', null, null, [
            'endpoint' => 'http://tos.cdn.com',
        ]);
        $this->assertSame('test', $tosAdapter->getBucket());
    }

    public function testSetBucket(): void
    {
        $tosAdapter = new TosAdapter($this->tosClient, self::CONFIG['bucket'], '', null, null, [
            'endpoint' => 'http://tos.cdn.com',
        ]);
        $tosAdapter->setBucket('new-bucket');
        $this->assertSame('new-bucket', $tosAdapter->getBucket());
    }

    public function testGetUrl(): void
    {
        $tosAdapter = new TosAdapter($this->tosClient, self::CONFIG['bucket'], '', null, null, [
            'endpoint' => 'http://tos.cdn.com',
        ]);
        $this->assertSame('http://test.tos.cdn.com/test', $tosAdapter->getUrl('test'));
    }

    public function testGetClient(): void
    {
        $tosAdapter = new TosAdapter($this->tosClient, self::CONFIG['bucket'], '', null, null, [
            'endpoint' => 'http://tos.cdn.com',
        ]);
        $this->assertSame($this->tosClient, $tosAdapter->getClient());
        $this->assertSame($this->tosClient, $tosAdapter->kernel());
    }

    public function testGetUrlWithoutSchema(): void
    {
        $tosAdapter = new TosAdapter($this->tosClient, self::CONFIG['bucket'], '', null, null, [
            'endpoint' => 'tos.cdn.com',
        ]);
        $this->assertSame('https://test.tos.cdn.com/test', $tosAdapter->getUrl('test'));
    }

    public function testGetUrlWithoutEndpoint(): void
    {
        $tosAdapter = new TosAdapter($this->tosClient, self::CONFIG['bucket'], '');
        $this->expectException(UnableToGetUrl::class);
        $this->expectExceptionMessage('Unable to get url with option endpoint missing.');
        $tosAdapter->getUrl('test');
    }

    public function testGetUrlWithUrl(): void
    {
        $tosAdapter = new TosAdapter($this->tosClient, self::CONFIG['bucket'], '', null, null, [
            'endpoint' => 'https://tos.cdn.com',
            'url' => 'https://tos.cdn.com',
        ]);
        $this->assertSame('https://tos.cdn.com/test', $tosAdapter->getUrl('test'));
    }

    public function testGetUrlWithBucketEndpoint(): void
    {
        $tosAdapter = new TosAdapter($this->tosClient, self::CONFIG['bucket'], '', null, null, [
            'endpoint' => 'https://tos.cdn.com',
            'bucket_endpoint' => true,
        ]);
        $this->assertSame('https://tos.cdn.com/test', $tosAdapter->getUrl('test'));
    }

    public function testGetTemporaryUrlWithUrl(): void
    {
        $tosAdapter = new TosAdapter($this->tosClient, self::CONFIG['bucket'], '', null, null, [
            'endpoint' => 'https://tos.cdn.com',
            'temporary_url' => 'https://tos.cdn.com',
        ]);
        $this->assertStringStartsWith('https://tos.cdn.com/test', $tosAdapter->getTemporaryUrl('test', 10));
    }

    public function testDirectoryExists(): void
    {
        if (! class_exists(UnableToCheckDirectoryExistence::class)) {
            $this->markTestSkipped('Require League Flysystem v3');
        }

        $this->expectException(UnableToCheckDirectoryExistence::class);
        $this->tosAdapter->directoryExists('path');
    }
}
