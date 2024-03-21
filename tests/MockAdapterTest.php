<?php

declare(strict_types=1);

namespace Zing\Flysystem\Tos\Tests;

use League\Flysystem\AdapterInterface;
use League\Flysystem\Config;
use Tos\Exception\TosServerException;
use Tos\Helper\StreamReader;
use Tos\Model\CopyObjectInput;
use Tos\Model\DeleteBucketOutput;
use Tos\Model\DeleteMultiObjectsInput;
use Tos\Model\DeleteObjectInput;
use Tos\Model\Enum;
use Tos\Model\GetObjectACLInput;
use Tos\Model\GetObjectACLOutput;
use Tos\Model\GetObjectInput;
use Tos\Model\GetObjectOutput;
use Tos\Model\Grant;
use Tos\Model\Grantee;
use Tos\Model\HeadObjectInput;
use Tos\Model\HeadObjectOutput;
use Tos\Model\ListedCommonPrefix;
use Tos\Model\ListedObject;
use Tos\Model\ListObjectsInput;
use Tos\Model\ListObjectsOutput;
use Tos\Model\PreSignedURLInput;
use Tos\Model\PreSignedURLOutput;
use Tos\Model\PutObjectACLInput;
use Tos\Model\PutObjectInput;
use Tos\TosClient;
use Zing\Flysystem\Tos\TosAdapter;

/**
 * @internal
 */
final class MockAdapterTest extends TestCase
{
    /**
     * @var \Mockery\MockInterface&\TOS\TosClient
     */
    private $client;

    /**
     * @var \Zing\Flysystem\Tos\TosAdapter
     */
    private $tosAdapter;

    protected function setUp(): void
    {
        parent::setUp();

        $this->client = \Mockery::mock(TosClient::class);
        $this->tosAdapter = new TosAdapter($this->client, 'test', '', [
            'endpoint' => 'tos-cn-shanghai.volces.com',
        ]);
        $this->client->shouldReceive('putObject')
            ->withArgs([\Mockery::on(static function (PutObjectInput $argument): bool {
                if ($argument->getBucket() !== 'test') {
                    return false;
                }

                if ($argument->getKey() !== 'fixture/read.txt') {
                    return false;
                }

                if ($argument->getContent() !== 'read-test') {
                    return false;
                }

                return $argument->getContentType() === 'text/plain';
            }),
            ])->andReturn([]);
        $this->tosAdapter->write('fixture/read.txt', 'read-test', new Config());
    }

    public function testUpdate(): void
    {
        $this->client->shouldReceive('putObject')
            ->withArgs([\Mockery::on(static function (PutObjectInput $argument): bool {
                if ($argument->getBucket() !== 'test') {
                    return false;
                }

                if ($argument->getKey() !== 'file.txt') {
                    return false;
                }

                if ($argument->getContent() !== 'update') {
                    return false;
                }

                return $argument->getContentType() === 'text/plain';
            }),
            ])->andReturn(null);
        $this->tosAdapter->update('file.txt', 'update', new Config());
        $output = \Mockery::mock(GetObjectOutput::class);
        $content = \Mockery::mock(StreamReader::class);
        $content->shouldReceive('getContents')
            ->andReturn('update');
        $output->shouldReceive('getContent')
            ->andReturn($content);
        $this->client->shouldReceive('getObject')
            ->withArgs([\Mockery::on(static function (GetObjectInput $argument): bool {
                if ($argument->getBucket() !== 'test') {
                    return false;
                }

                return $argument->getKey() === 'file.txt';
            }),
            ])->andReturn($output);
        $this->assertSame('update', $this->tosAdapter->read('file.txt')['contents']);
    }

    public function testUpdateStream(): void
    {
        $this->client->shouldReceive('putObject')
            ->withArgs([\Mockery::on(static function (PutObjectInput $argument): bool {
                if ($argument->getBucket() !== 'test') {
                    return false;
                }

                if ($argument->getKey() !== 'file.txt') {
                    return false;
                }

                if ($argument->getContent() !== 'write') {
                    return false;
                }

                return $argument->getContentType() === 'text/plain';
            }),
            ])->andReturn(null);
        $this->tosAdapter->write('file.txt', 'write', new Config());
        $stream = $this->streamFor('update')
            ->detach();
        $this->client->shouldReceive('putObject')
            ->withArgs([\Mockery::on(static function (PutObjectInput $argument) use ($stream): bool {
                if ($argument->getBucket() !== 'test') {
                    return false;
                }

                if ($argument->getKey() !== 'file.txt') {
                    return false;
                }

                if ($argument->getContent() !== $stream) {
                    return false;
                }

                return $argument->getContentType() === 'text/plain';
            }),
            ])->andReturn(null);
        $this->tosAdapter->updateStream('file.txt', $stream, new Config());
        $output = \Mockery::mock(GetObjectOutput::class);
        $content = \Mockery::mock(StreamReader::class);
        $content->shouldReceive('getContents')
            ->andReturn('update');
        $output->shouldReceive('getContent')
            ->andReturn($content);
        $this->client->shouldReceive('getObject')
            ->withArgs([\Mockery::on(static function (GetObjectInput $argument): bool {
                if ($argument->getBucket() !== 'test') {
                    return false;
                }

                return $argument->getKey() === 'file.txt';
            }),
            ])->andReturn($output);
        $this->assertSame('update', $this->tosAdapter->read('file.txt')['contents']);
    }

    private function mockPutObject(string $path, $body, ?string $visibility = null): void
    {
        $this->client->shouldReceive('putObject')
            ->withArgs([\Mockery::on(static function (PutObjectInput $argument) use ($path, $body, $visibility): bool {
                if ($argument->getBucket() !== 'test') {
                    return false;
                }

                if ($argument->getKey() !== $path) {
                    return false;
                }

                if ($argument->getContent() !== $body) {
                    return false;
                }

                if ($argument->getContentType() !== ($body !== '' ? 'text/plain' : null)) {
                    return false;
                }

                $acl = null;
                if ($visibility !== null && $visibility !== '') {
                    $acl = $visibility === AdapterInterface::VISIBILITY_PUBLIC ? Enum::ACLPublicRead : Enum::ACLPrivate;
                }

                return $argument->getACL() === $acl;
            }),
            ])
            ->andReturn(null);
    }

    public function testCopy(): void
    {
        $this->mockPutObject('file.txt', 'write');
        $this->tosAdapter->write('file.txt', 'write', new Config());
        $this->client->shouldReceive('copyObject')
            ->withArgs([
                \Mockery::on(static function (CopyObjectInput $copyObjectInput): bool {
                    if ($copyObjectInput->getBucket() !== 'test') {
                        return false;
                    }

                    if ($copyObjectInput->getKey() !== 'copy.txt') {
                        return false;
                    }

                    if ($copyObjectInput->getSrcBucket() !== 'test') {
                        return false;
                    }

                    return $copyObjectInput->getSrcKey() === 'file.txt';
                }),
            ])->andReturn(null);
        $this->mockGetVisibility('file.txt', AdapterInterface::VISIBILITY_PUBLIC);
        $this->tosAdapter->copy('file.txt', 'copy.txt');
        $this->mockGetObject('copy.txt', 'write');
        $this->assertSame('write', $this->tosAdapter->read('copy.txt')['contents']);
    }

    private function mockGetObject(string $path, string $body): void
    {
        $output = \Mockery::mock(GetObjectOutput::class);
        $content = \Mockery::mock(StreamReader::class);
        $content->shouldReceive('getContents')
            ->andReturn($body);
        $output->shouldReceive('getContent')
            ->andReturn($content);
        $this->client->shouldReceive('getObject')
            ->withArgs([\Mockery::on(static function (GetObjectInput $headObjectInput) use ($path): bool {
                if ($headObjectInput->getBucket() !== 'test') {
                    return false;
                }

                return $headObjectInput->getKey() === $path;
            }),
            ])->andReturn($output);
    }

    public function testCreateDir(): void
    {
        $this->mockPutObject('path/', '');
        $this->tosAdapter->createDir('path', new Config());
        $output = \Mockery::mock(ListObjectsOutput::class);
        $output->shouldReceive('getNextMarker')
            ->andReturn('');
        $output->shouldReceive('getContents')
            ->andReturn([]);
        $output->shouldReceive('getCommonPrefixes')
            ->andReturn([]);

        $this->client->shouldReceive('listObjects')
            ->withArgs([\Mockery::on(static function (ListObjectsInput $listObjectsInput): bool {
                if ($listObjectsInput->getBucket() !== 'test') {
                    return false;
                }

                if ($listObjectsInput->getPrefix() !== 'path/') {
                    return false;
                }

                if ($listObjectsInput->getMaxKeys() !== 1000) {
                    return false;
                }

                if ($listObjectsInput->getDelimiter() !== '/') {
                    return false;
                }

                return $listObjectsInput->getMarker() === '';
            }),
            ])->andReturn($output);
        $this->client->shouldReceive('getObjectMetadata')
            ->withArgs([['test', 'path/']])->andReturn([
                'ContentLength' => 0,
                'Date' => 'Mon, 31 May 2021 06:52:32 GMT',
                'RequestId' => '00000179C13207EF9217A7F5589D2DC6',
                'Id2' => '32AAAQAAEAABAAAQAAEAABAAAQAAEAABCSvXM+dHYwFYYJv2m9y5LibcMVibe3QN',
                'Reserved' => 'amazon, aws and amazon web services are trademarks or registered trademarks of Amazon Technologies, Inc',
                'Expiration' => '',
                'last-modified' => 'Mon, 31 May 2021 06:52:31 GMT',
                'content-type' => 'binary/octet-stream',
                'ETag' => 'd41d8cd98f00b204e9800998ecf8427e',
                'VersionId' => '',
                'WebsiteRedirectLocation' => '',
                'StorageClass' => 'STANDARD_IA',
                'AllowOrigin' => '',
                'MaxAgeSeconds' => '',
                'ExposeHeader' => '',
                'AllowMethod' => '',
                'AllowHeader' => '',
                'Restore' => '',
                'SseKms' => '',
                'SseKmsKey' => '',
                'SseC' => '',
                'SseCKeyMd5' => '',
                'Metadata' => [],
                'HttpStatusCode' => 200,
                'Reason' => 'OK',
            ]);
        $this->assertSame([], $this->tosAdapter->listContents('path'));
    }

    public function testSetVisibility(): void
    {
        $this->mockPutObject('file.txt', 'write');
        $this->tosAdapter->write('file.txt', 'write', new Config());
        $this->mockGetVisibility('file.txt', AdapterInterface::VISIBILITY_PRIVATE);
        $this->assertSame(
            AdapterInterface::VISIBILITY_PRIVATE,
            $this->tosAdapter->getVisibility('file.txt')['visibility']
        );
        $this->client->shouldReceive('putObjectACL')
            ->withArgs([\Mockery::on(static function (PutObjectACLInput $putObjectACLInput): bool {
                if ($putObjectACLInput->getBucket() !== 'test') {
                    return false;
                }

                if ($putObjectACLInput->getKey() !== 'file.txt') {
                    return false;
                }

                return $putObjectACLInput->getACL() === Enum::ACLPublicRead;
            }),
            ])->andReturn(null);
        $this->tosAdapter->setVisibility('file.txt', AdapterInterface::VISIBILITY_PUBLIC);
        $this->mockGetVisibility('file.txt', AdapterInterface::VISIBILITY_PUBLIC);
        $this->assertSame(
            AdapterInterface::VISIBILITY_PUBLIC,
            $this->tosAdapter->getVisibility('file.txt')['visibility']
        );
    }

    public function testRename(): void
    {
        $this->mockPutObject('from.txt', 'write');
        $this->tosAdapter->write('from.txt', 'write', new Config());
        $this->mockGetMetadata('from.txt');
        $this->assertTrue($this->tosAdapter->has('from.txt'));
        $this->client->shouldReceive('headObject')
            ->once()
            ->withArgs([\Mockery::on(static function (HeadObjectInput $headObjectInput): bool {
                if ($headObjectInput->getBucket() !== 'test') {
                    return false;
                }

                return $headObjectInput->getKey() === 'to.txt';
            }),
            ])->andThrow(new TosServerException());
        $this->assertFalse($this->tosAdapter->has('to.txt'));
        $this->client->shouldReceive('copyObject')
            ->withArgs([
                \Mockery::on(static function (CopyObjectInput $copyObjectInput): bool {
                    if ($copyObjectInput->getBucket() !== 'test') {
                        return false;
                    }

                    if ($copyObjectInput->getKey() !== 'to.txt') {
                        return false;
                    }

                    if ($copyObjectInput->getSrcBucket() !== 'test') {
                        return false;
                    }

                    return $copyObjectInput->getSrcKey() === 'from.txt';
                }),
            ])->andReturn(null);
        $this->client->shouldReceive('deleteObject')
            ->withArgs([\Mockery::on(static function (DeleteObjectInput $deleteObjectInput): bool {
                if ($deleteObjectInput->getBucket() !== 'test') {
                    return false;
                }

                return $deleteObjectInput->getKey() === 'from.txt';
            }),
            ])->andReturn(\Mockery::mock(DeleteBucketOutput::class));
        $this->mockGetVisibility('from.txt', AdapterInterface::VISIBILITY_PUBLIC);
        $this->tosAdapter->rename('from.txt', 'to.txt');
        $this->client->shouldReceive('headObject')
            ->once()
            ->withArgs([\Mockery::on(static function (HeadObjectInput $headObjectInput): bool {
                if ($headObjectInput->getBucket() !== 'test') {
                    return false;
                }

                return $headObjectInput->getKey() === 'from.txt';
            }),
            ])->andThrow(new TosServerException());
        $this->assertFalse($this->tosAdapter->has('from.txt'));
        $this->mockGetObject('to.txt', 'write');
        $this->assertSame('write', $this->tosAdapter->read('to.txt')['contents']);
        $this->client->shouldReceive('deleteObject')
            ->withArgs([\Mockery::on(static function (DeleteObjectInput $deleteObjectInput): bool {
                if ($deleteObjectInput->getBucket() !== 'test') {
                    return false;
                }

                return $deleteObjectInput->getKey() === 'to.txt';
            }),
            ])->andReturn(null);
        $this->tosAdapter->delete('to.txt');
    }

    public function testDeleteDir(): void
    {
        $output = \Mockery::mock(ListObjectsOutput::class);
        $output->shouldReceive('getNextMarker')
            ->andReturn('');
        $owner = null;
        $output->shouldReceive('getContents')
            ->andReturn([new ListedObject(
                'path/',
                '2021-05-31T06:52:31.942Z',
                '"d41d8cd98f00b204e9800998ecf8427e"',
                0,
                $owner,
                'STANDARD_IA'
            ), new ListedObject(
                'path/file.txt',
                '2021-05-31T06:52:32.001Z',
                '"098f6bcd4621d373cade4e832627b4f6"',
                4,
                $owner,
                'STANDARD_IA'
            ),
            ]);
        $output->shouldReceive('getCommonPrefixes')
            ->andReturn([]);
        $this->client->shouldReceive('listObjects')
            ->withArgs([\Mockery::on(static function (ListObjectsInput $listObjectsInput): bool {
                if ($listObjectsInput->getBucket() !== 'test') {
                    return false;
                }

                if ($listObjectsInput->getPrefix() !== 'path/') {
                    return false;
                }

                if ($listObjectsInput->getMaxKeys() !== 1000) {
                    return false;
                }

                if ($listObjectsInput->getDelimiter() !== '') {
                    return false;
                }

                return $listObjectsInput->getMarker() === '';
            }),
            ])->andReturn($output);
        $this->mockGetMetadata('path/');
        $this->mockGetMetadata('path/file.txt');
        $this->client->shouldReceive('deleteMultiObjects')
            ->withArgs([\Mockery::on(static function (DeleteMultiObjectsInput $deleteMultiObjectsInput): bool {
                if ($deleteMultiObjectsInput->getBucket() !== 'test') {
                    return false;
                }

                if ($deleteMultiObjectsInput->getObjects()[0]->getKey() !== 'path/') {
                    return false;
                }

                return $deleteMultiObjectsInput->getObjects()[1]
                    ->getKey() === 'path/file.txt';
            }),
            ])->andReturn(null);
        $this->assertTrue($this->tosAdapter->deleteDir('path'));
    }

    public function testWriteStream(): void
    {
        $stream = $this->streamFor('write')
            ->detach();
        $this->mockPutObject('file.txt', $stream);
        $this->tosAdapter->writeStream('file.txt', $stream, new Config());
        $this->mockGetObject('file.txt', 'write');
        $this->assertSame('write', $this->tosAdapter->read('file.txt')['contents']);
    }

    /**
     * @return \Iterator<string[]>
     */
    public function provideWriteStreamWithVisibilityCases(): \Iterator
    {
        yield [AdapterInterface::VISIBILITY_PUBLIC];

        yield [AdapterInterface::VISIBILITY_PRIVATE];
    }

    private function mockGetVisibility(string $path, string $visibility): void
    {
        $output = \Mockery::mock(GetObjectACLOutput::class);
        $grantee = (new Grantee());
        $grantee->setCanned(Enum::CannedAllUsers);
        $output->shouldReceive('getGrants')
            ->andReturn(
                $visibility === AdapterInterface::VISIBILITY_PUBLIC ? [new Grant($grantee, Enum::PermissionRead)] : []
            );
        $this->client->shouldReceive('getObjectACL')
            ->once()
            ->withArgs([\Mockery::on(static function (GetObjectACLInput $getObjectOutput) use ($path): bool {
                if ($getObjectOutput->getBucket() !== 'test') {
                    return false;
                }

                return $getObjectOutput->getKey() === $path;
            }),
            ])
            ->andReturn($output);
    }

    /**
     * @dataProvider provideWriteStreamWithVisibilityCases
     */
    public function testWriteStreamWithVisibility(string $visibility): void
    {
        $stream = $this->streamFor('write')
            ->detach();
        $this->mockPutObject('file.txt', $stream, $visibility);
        $this->tosAdapter->writeStream('file.txt', $stream, new Config([
            'visibility' => $visibility,
        ]));
        $this->mockGetVisibility('file.txt', $visibility);
        $this->assertSame($visibility, $this->tosAdapter->getVisibility('file.txt')['visibility']);
    }

    public function testWriteStreamWithExpires(): void
    {
        $stream = $this->streamFor('write')
            ->detach();
        $this->client->shouldReceive('putObject')
            ->withArgs([\Mockery::on(static function (PutObjectInput $argument) use ($stream): bool {
                if ($argument->getBucket() !== 'test') {
                    return false;
                }

                if ($argument->getKey() !== 'file.txt') {
                    return false;
                }

                if ($argument->getContent() !== $stream) {
                    return false;
                }

                if ($argument->getContentType() !== 'text/plain') {
                    return false;
                }

                return $argument->getExpires() === 20;
            }),
            ])
            ->andReturn(null);
        $this->tosAdapter->writeStream('file.txt', $stream, new Config([
            'Expires' => 20,
        ]));
        $this->mockGetObject('file.txt', 'write');
        $this->assertSame('write', $this->tosAdapter->read('file.txt')['contents']);
    }

    public function testWriteStreamWithMimetype(): void
    {
        $stream = $this->streamFor('write')
            ->detach();
        $this->client->shouldReceive('putObject')
            ->withArgs([
                \Mockery::on(static function (PutObjectInput $argument) use ($stream): bool {
                    if ($argument->getBucket() !== 'test') {
                        return false;
                    }

                    if ($argument->getKey() !== 'file.txt') {
                        return false;
                    }

                    if ($argument->getContent() !== $stream) {
                        return false;
                    }

                    return $argument->getContentType() === 'image/png';
                }),
            ])->andReturn(null);
        $this->tosAdapter->writeStream('file.txt', $stream, new Config([
            'mimetype' => 'image/png',
        ]));
        $output = \Mockery::mock(HeadObjectOutput::class);
        $output->shouldReceive('getContentLength')
            ->andReturn(9);
        $output->shouldReceive('getLastModified')
            ->andReturn(1622443952);
        $output->shouldReceive('getContentType')
            ->andReturn('image/png');
        $output->shouldReceive('getETag')
            ->andReturn('d41d8cd98f00b204e9800998ecf8427e');
        $output->shouldReceive('getStorageClass')
            ->andReturn('STANDARD');
        $this->client->shouldReceive('headObject')
            ->once()
            ->withArgs([\Mockery::on(static function (HeadObjectInput $headObjectInput): bool {
                if ($headObjectInput->getBucket() !== 'test') {
                    return false;
                }

                return $headObjectInput->getKey() === 'file.txt';
            }),
            ])->andReturn($output);
        $this->assertSame('image/png', $this->tosAdapter->getMimetype('file.txt')['mimetype']);
    }

    public function testDelete(): void
    {
        $stream = $this->streamFor('write')
            ->detach();
        $this->mockPutObject('file.txt', $stream);
        $this->tosAdapter->writeStream('file.txt', $stream, new Config());
        $this->mockGetMetadata('file.txt');
        $this->assertTrue($this->tosAdapter->has('file.txt'));
        $this->client->shouldReceive('deleteObject')
            ->withArgs([\Mockery::on(static function (DeleteObjectInput $deleteObjectInput): bool {
                if ($deleteObjectInput->getBucket() !== 'test') {
                    return false;
                }

                return $deleteObjectInput->getKey() === 'file.txt';
            }),
            ])->andReturn(null);
        $this->tosAdapter->delete('file.txt');
        $this->client->shouldReceive('headObject')
            ->once()
            ->withArgs([\Mockery::on(static function (HeadObjectInput $headObjectInput): bool {
                if ($headObjectInput->getBucket() !== 'test') {
                    return false;
                }

                return $headObjectInput->getKey() === 'file.txt';
            }),
            ])->andThrow(new TosServerException());
        $this->assertFalse($this->tosAdapter->has('file.txt'));
    }

    public function testWrite(): void
    {
        $this->mockPutObject('file.txt', 'write');
        $this->tosAdapter->write('file.txt', 'write', new Config());
        $this->mockGetObject('file.txt', 'write');
        $this->assertSame('write', $this->tosAdapter->read('file.txt')['contents']);
    }

    public function testRead(): void
    {
        $this->mockGetObject('fixture/read.txt', 'read-test');
        $this->assertSame('read-test', $this->tosAdapter->read('fixture/read.txt')['contents']);
    }

    public function testReadStream(): void
    {
        $output = \Mockery::mock(GetObjectOutput::class);
        $content = \Mockery::mock(StreamReader::class);
        $content->shouldReceive('detach')
            ->andReturn($this->streamFor('read-test')->detach());
        $output->shouldReceive('getContent')
            ->andReturn($content);
        $this->client->shouldReceive('getObject')
            ->withArgs([\Mockery::on(static function (GetObjectInput $getObjectInput): bool {
                if ($getObjectInput->getBucket() !== 'test') {
                    return false;
                }

                return $getObjectInput->getKey() === 'fixture/read.txt';
            }),
            ])->andReturn($output);
        $this->assertSame(
            'read-test',
            stream_get_contents($this->tosAdapter->readStream('fixture/read.txt')['stream'])
        );
    }

    public function testGetVisibility(): void
    {
        $this->mockGetVisibility('fixture/read.txt', AdapterInterface::VISIBILITY_PRIVATE);
        $this->assertSame(
            AdapterInterface::VISIBILITY_PRIVATE,
            $this->tosAdapter->getVisibility('fixture/read.txt')['visibility']
        );
    }

    public function testGetMetadata(): void
    {
        $this->mockGetMetadata('fixture/read.txt');
        $this->assertIsArray($this->tosAdapter->getMetadata('fixture/read.txt'));
    }

    private function mockGetMetadata(string $path): void
    {
        $output = \Mockery::mock(HeadObjectOutput::class);
        $output->shouldReceive('getContentLength')
            ->andReturn(9);
        $output->shouldReceive('getLastModified')
            ->andReturn(1622443952);
        $output->shouldReceive('getContentType')
            ->andReturn('text/plain');
        $output->shouldReceive('getETag')
            ->andReturn('d41d8cd98f00b204e9800998ecf8427e');
        $output->shouldReceive('getStorageClass')
            ->andReturn('STANDARD');
        $this->client->shouldReceive('headObject')
            ->once()
            ->withArgs([\Mockery::on(static function (HeadObjectInput $headObjectInput) use ($path): bool {
                if ($headObjectInput->getBucket() !== 'test') {
                    return false;
                }

                return $headObjectInput->getKey() === $path;
            }),
            ])->andReturn($output);
    }

    public function testListContents(): void
    {
        $this->mockPutObject('path/', '');
        $this->tosAdapter->createDir('path', new Config());
        $output = \Mockery::mock(ListObjectsOutput::class);
        $output->shouldReceive('getNextMarker')
            ->andReturn('');
        $owner = null;
        $output->shouldReceive('getContents')
            ->andReturn([new ListedObject(
                'path/file.txt',
                1622443952,
                'd41d8cd98f00b204e9800998ecf8427e',
                9,
                $owner,
                'STANDARD_IA'
            ),
            ]);
        $output->shouldReceive('getCommonPrefixes')
            ->andReturn([]);
        $this->client->shouldReceive('listObjects')
            ->withArgs([\Mockery::on(static function (ListObjectsInput $listObjectsInput): bool {
                if ($listObjectsInput->getBucket() !== 'test') {
                    return false;
                }

                if ($listObjectsInput->getPrefix() !== 'path/') {
                    return false;
                }

                if ($listObjectsInput->getMaxKeys() !== 1000) {
                    return false;
                }

                if ($listObjectsInput->getDelimiter() !== '/') {
                    return false;
                }

                return $listObjectsInput->getMarker() === '';
            }),
            ])->andReturn($output);
        $this->client->shouldReceive('getObjectMetadata')
            ->withArgs([['test', 'path/']])->andReturn([
                'ContentLength' => 0,
                'Date' => 'Mon, 31 May 2021 06:52:32 GMT',
                'RequestId' => '00000179C13207EF9217A7F5589D2DC6',
                'Id2' => '32AAAQAAEAABAAAQAAEAABAAAQAAEAABCSvXM+dHYwFYYJv2m9y5LibcMVibe3QN',
                'Reserved' => 'amazon, aws and amazon web services are trademarks or registered trademarks of Amazon Technologies, Inc',
                'Expiration' => '',
                'last-modified' => 'Mon, 31 May 2021 06:52:31 GMT',
                'content-type' => 'binary/octet-stream',
                'ETag' => 'd41d8cd98f00b204e9800998ecf8427e',
                'VersionId' => '',
                'WebsiteRedirectLocation' => '',
                'StorageClass' => 'STANDARD_IA',
                'AllowOrigin' => '',
                'MaxAgeSeconds' => '',
                'ExposeHeader' => '',
                'AllowMethod' => '',
                'AllowHeader' => '',
                'Restore' => '',
                'SseKms' => '',
                'SseKmsKey' => '',
                'SseC' => '',
                'SseCKeyMd5' => '',
                'Metadata' => [],
                'HttpStatusCode' => 200,
                'Reason' => 'OK',
            ]);
        $this->assertNotEmpty($this->tosAdapter->listContents('path'));
        $output = \Mockery::mock(ListObjectsOutput::class);
        $output->shouldReceive('getNextMarker')
            ->andReturn('');
        $output->shouldReceive('getContents')
            ->andReturn([]);
        $output->shouldReceive('getCommonPrefixes')
            ->andReturn([]);
        $this->client->shouldReceive('listObjects')
            ->withArgs([\Mockery::on(static function (ListObjectsInput $listObjectsInput): bool {
                if ($listObjectsInput->getBucket() !== 'test') {
                    return false;
                }

                if ($listObjectsInput->getPrefix() !== 'path1/') {
                    return false;
                }

                if ($listObjectsInput->getMaxKeys() !== 1000) {
                    return false;
                }

                if ($listObjectsInput->getDelimiter() !== '/') {
                    return false;
                }

                return $listObjectsInput->getMarker() === '';
            }),
            ])->andReturn($output);
        $this->assertEmpty($this->tosAdapter->listContents('path1'));
        $this->mockPutObject('a/b/file.txt', 'test');
        $this->tosAdapter->write('a/b/file.txt', 'test', new Config());
        $output = \Mockery::mock(ListObjectsOutput::class);
        $output->shouldReceive('getNextMarker')
            ->andReturn('');
        $owner = null;
        $output->shouldReceive('getContents')
            ->andReturn([new ListedObject(
                'a/b/file.txt',
                1622443952,
                'd41d8cd98f00b204e9800998ecf8427e',
                9,
                $owner,
                'STANDARD_IA'
            ),
            ]);
        $output->shouldReceive('getCommonPrefixes')
            ->andReturn([new ListedCommonPrefix('a/b/')]);
        $this->client->shouldReceive('listObjects')
            ->withArgs([\Mockery::on(static function (ListObjectsInput $listObjectsInput): bool {
                if ($listObjectsInput->getBucket() !== 'test') {
                    return false;
                }

                if ($listObjectsInput->getPrefix() !== 'a/') {
                    return false;
                }

                if ($listObjectsInput->getMaxKeys() !== 1000) {
                    return false;
                }

                if ($listObjectsInput->getDelimiter() !== '') {
                    return false;
                }

                return $listObjectsInput->getMarker() === '';
            }),
            ])->andReturn($output);

        $this->mockGetMetadata('a/b/file.txt');
        $this->assertSame([
            [
                'type' => 'file',
                'mimetype' => null,
                'path' => 'a/b/file.txt',
                'timestamp' => 1622443952,
                'size' => 9,
            ], [
                'type' => 'dir',
                'path' => 'a/b',
            ],
        ], $this->tosAdapter->listContents('a', true));
    }

    public function testGetSize(): void
    {
        $this->mockGetMetadata('fixture/read.txt');
        $this->assertSame(9, $this->tosAdapter->getSize('fixture/read.txt')['size']);
    }

    public function testGetTimestamp(): void
    {
        $this->mockGetMetadata('fixture/read.txt');
        $this->assertSame(1622443952, $this->tosAdapter->getTimestamp('fixture/read.txt')['timestamp']);
    }

    public function testGetMimetype(): void
    {
        $this->mockGetMetadata('fixture/read.txt');
        $this->assertSame('text/plain', $this->tosAdapter->getMimetype('fixture/read.txt')['mimetype']);
    }

    public function testHas(): void
    {
        $this->mockGetMetadata('fixture/read.txt');
        $this->assertTrue($this->tosAdapter->has('fixture/read.txt'));
    }

    public function testSignUrl(): void
    {
        $output = \Mockery::mock(PreSignedURLOutput::class);
        $output->shouldReceive('getSignedUrl')
            ->andReturn('signed-url');
        $this->client->shouldReceive('preSignedURL')
            ->withArgs([\Mockery::on(static function (PreSignedURLInput $preSignedURLInput): bool {
                if ($preSignedURLInput->getBucket() !== 'test') {
                    return false;
                }

                if ($preSignedURLInput->getKey() !== 'fixture/read.txt') {
                    return false;
                }

                if ($preSignedURLInput->getExpires() !== 10) {
                    return false;
                }

                if ($preSignedURLInput->getHttpMethod() !== 'GET') {
                    return false;
                }

                return $preSignedURLInput->getQuery() === [];
            }),
            ])->andReturn($output);
        $this->assertSame('signed-url', $this->tosAdapter->signUrl('fixture/read.txt', 10, []));
    }

    public function testGetTemporaryUrl(): void
    {
        $output = \Mockery::mock(PreSignedURLOutput::class);
        $output->shouldReceive('getSignedUrl')
            ->andReturn('signed-url');
        $this->client->shouldReceive('preSignedURL')
            ->withArgs([\Mockery::on(static function (PreSignedURLInput $preSignedURLInput): bool {
                if ($preSignedURLInput->getBucket() !== 'test') {
                    return false;
                }

                if ($preSignedURLInput->getKey() !== 'fixture/read.txt') {
                    return false;
                }

                if ($preSignedURLInput->getExpires() !== 10) {
                    return false;
                }

                if ($preSignedURLInput->getHttpMethod() !== 'GET') {
                    return false;
                }

                return $preSignedURLInput->getQuery() === [];
            }),
            ])->andReturn($output);
        $this->assertSame('signed-url', $this->tosAdapter->getTemporaryUrl('fixture/read.txt', 10, []));
    }
}
