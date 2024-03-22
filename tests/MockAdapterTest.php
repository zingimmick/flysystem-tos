<?php

declare(strict_types=1);

namespace Zing\Flysystem\Tos\Tests;

use League\Flysystem\Config;
use League\Flysystem\DirectoryAttributes;
use League\Flysystem\FileAttributes;
use League\Flysystem\StorageAttributes;
use League\Flysystem\UnableToCopyFile;
use League\Flysystem\UnableToDeleteDirectory;
use League\Flysystem\UnableToRetrieveMetadata;
use League\Flysystem\Visibility;
use Tos\Helper\StreamReader;
use Tos\Model\Constant;
use Tos\Model\CopyObjectInput;
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
     * @var \Mockery\MockInterface&\Tos\TosClient
     */
    private $client;

    private TosAdapter $tosAdapter;

    protected function setUp(): void
    {
        parent::setUp();

        $this->client = \Mockery::mock(TosClient::class);
        $this->tosAdapter = new TosAdapter($this->client, 'test');
        $this->mockPutObject('fixture/read.txt', 'read-test');
        $this->tosAdapter->write('fixture/read.txt', 'read-test', new Config());
    }

    /**
     * @param resource|string $body
     */
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
                if ($visibility !== null && $visibility !== '' && $visibility !== '0') {
                    $acl = $visibility === Visibility::PUBLIC ? Enum::ACLPublicRead : Enum::ACLPrivate;
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
        $this->mockGetVisibility('file.txt', Visibility::PUBLIC);
        $this->tosAdapter->copy('file.txt', 'copy.txt', new Config());
        $this->mockGetObject('copy.txt', 'write');
        $this->assertSame('write', $this->tosAdapter->read('copy.txt'));
    }

    public function testCopyWithoutRetainVisibility(): void
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

                    if ($copyObjectInput->getACL() !== Enum::ACLPrivate) {
                        return false;
                    }

                    return $copyObjectInput->getSrcKey() === 'file.txt';
                }),
            ])->andReturn(null);
        $this->mockGetVisibility('file.txt', Visibility::PUBLIC);
        $this->tosAdapter->copy('file.txt', 'copy.txt', new Config([
            'retain_visibility' => false,
        ]));
        $this->mockGetVisibility('copy.txt', Visibility::PRIVATE);
        $this->assertSame(Visibility::PRIVATE, $this->tosAdapter->visibility('copy.txt')->visibility());
    }

    public function testCopyFailed(): void
    {
        $this->mockPutObject('file.txt', 'write');
        $this->tosAdapter->write('file.txt', 'write', new Config());
        $this->client->shouldReceive('copyObject')
            ->withArgs([\Mockery::on(static function (CopyObjectInput $copyObjectInput): bool {
                if ($copyObjectInput->getBucket() !== 'test') {
                    return false;
                }

                if ($copyObjectInput->getKey() !== 'copy.txt') {
                    return false;
                }

                if ($copyObjectInput->getSrcBucket() !== 'test') {
                    return false;
                }

                if ($copyObjectInput->getACL() !== Enum::ACLPublicRead) {
                    return false;
                }

                return $copyObjectInput->getSrcKey() === 'file.txt';
            }),
            ])->andThrow(new \Tos\Exception\TosServerException());
        $this->mockGetVisibility('file.txt', Visibility::PUBLIC);
        $this->expectException(UnableToCopyFile::class);
        $this->tosAdapter->copy('file.txt', 'copy.txt', new Config());
        $this->mockGetObject('copy.txt', 'write');
        $this->assertSame('write', $this->tosAdapter->read('copy.txt'));
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
        $this->mockPutObject('path/', '', Visibility::PUBLIC);
        $output = \Mockery::mock(ListObjectsOutput::class);
        $output->shouldReceive('getNextMarker')
            ->andReturn('');
        $output->shouldReceive('getContents')
            ->andReturn([new ListedObject('path/')]);
        $output->shouldReceive('getCommonPrefixes')
            ->andReturn([]);
        $this->client->shouldReceive('listObjects')
            ->once()
            ->withArgs([\Mockery::on(static function (ListObjectsInput $listObjectsInput): bool {
                if ($listObjectsInput->getBucket() !== 'test') {
                    return false;
                }

                if ($listObjectsInput->getPrefix() !== 'path/') {
                    return false;
                }

                if ($listObjectsInput->getMaxKeys() !== 1) {
                    return false;
                }

                if ($listObjectsInput->getDelimiter() !== '/') {
                    return false;
                }

                return $listObjectsInput->getMarker() === '';
            }),
            ])->andReturn($output);
        $output = \Mockery::mock(ListObjectsOutput::class);
        $output->shouldReceive('getNextMarker')
            ->andReturn('');
        $output->shouldReceive('getContents')
            ->andReturn([new ListedObject('path/')]);
        $output->shouldReceive('getCommonPrefixes')
            ->andReturn([]);
        $this->client->shouldReceive('listObjects')
            ->once()
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
        $this->client->shouldReceive('listObjects')
            ->once()
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
        $this->client->shouldReceive('deleteMultiObjects')
            ->once()
            ->withArgs([\Mockery::on(static function (DeleteMultiObjectsInput $deleteMultiObjectsInput): bool {
                if ($deleteMultiObjectsInput->getBucket() !== 'test') {
                    return false;
                }

                return $deleteMultiObjectsInput->getObjects()[0]
                    ->getKey() === 'path/';
            }),
            ]);
        $output = \Mockery::mock(ListObjectsOutput::class);
        $output->shouldReceive('getNextMarker')
            ->andReturn('');
        $output->shouldReceive('getContents')
            ->andReturn([]);
        $output->shouldReceive('getCommonPrefixes')
            ->andReturn([]);
        $this->client->shouldReceive('listObjects')
            ->once()
            ->withArgs([\Mockery::on(static function (ListObjectsInput $listObjectsInput): bool {
                if ($listObjectsInput->getBucket() !== 'test') {
                    return false;
                }

                if ($listObjectsInput->getPrefix() !== 'path/') {
                    return false;
                }

                if ($listObjectsInput->getMaxKeys() !== 1) {
                    return false;
                }

                if ($listObjectsInput->getDelimiter() !== '/') {
                    return false;
                }

                return $listObjectsInput->getMarker() === '';
            }),
            ])->andReturn($output);
        $this->tosAdapter->createDirectory('path', new Config());
        $this->assertTrue($this->tosAdapter->directoryExists('path'));
        $this->assertSame([], iterator_to_array($this->tosAdapter->listContents('path', false)));
        $this->tosAdapter->deleteDirectory('path');
        $this->assertFalse($this->tosAdapter->directoryExists('path'));
    }

    public function testSetVisibility(): void
    {
        $this->mockPutObject('file.txt', 'write');
        $this->tosAdapter->write('file.txt', 'write', new Config());
        $this->mockGetVisibility('file.txt', Visibility::PRIVATE);
        $this->assertSame(Visibility::PRIVATE, $this->tosAdapter->visibility('file.txt')->visibility());
        $this->client->shouldReceive('putObjectACL')
            ->withArgs([\Mockery::on(static function (PutObjectACLInput $argument): bool {
                if ($argument->getBucket() !== 'test') {
                    return false;
                }

                if ($argument->getKey() !== 'file.txt') {
                    return false;
                }

                return $argument->getACL() === Enum::ACLPublicRead;
            }),
            ])->andReturn(null);
        $this->tosAdapter->setVisibility('file.txt', Visibility::PUBLIC);
        $this->mockGetVisibility('file.txt', Visibility::PUBLIC);

        $this->assertSame(Visibility::PUBLIC, $this->tosAdapter->visibility('file.txt')['visibility']);
    }

    public function testRename(): void
    {
        $this->mockPutObject('from.txt', 'write');
        $this->tosAdapter->write('from.txt', 'write', new Config());
        $this->mockGetMetadata('from.txt');
        $this->assertTrue($this->tosAdapter->fileExists('from.txt'));
        $this->mockGetEmptyMetadata('to.txt');
        $this->assertFalse($this->tosAdapter->fileExists('to.txt'));
        $this->client->shouldReceive('copyObject')
            ->withArgs([\Mockery::on(static function (CopyObjectInput $copyObjectInput): bool {
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
            ])->andReturn(null);
        $this->mockGetVisibility('from.txt', Visibility::PUBLIC);
        $this->tosAdapter->move('from.txt', 'to.txt', new Config());
        $this->mockGetEmptyMetadata('from.txt');
        $this->assertFalse($this->tosAdapter->fileExists('from.txt'));
        $this->mockGetObject('to.txt', 'write');
        $this->assertSame('write', $this->tosAdapter->read('to.txt'));
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
                1622443952,
                'd41d8cd98f00b204e9800998ecf8427e',
                0,
                $owner,
                'STANDARD_IA'
            ), new ListedObject(
                'path/file.txt',
                1622443952,
                '098f6bcd4621d373cade4e832627b4f6',
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
        $this->client->shouldReceive('deleteMultiObjects')
            ->once()
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
        $this->client->shouldReceive('deleteMultiObjects')
            ->once()
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
            ])
            ->andThrow(new \Tos\Exception\TosServerException());
        $this->tosAdapter->deleteDirectory('path');
        $this->expectException(UnableToDeleteDirectory::class);
        $this->tosAdapter->deleteDirectory('path');
        $this->assertTrue(true);
    }

    public function testWriteStream(): void
    {
        $contents = $this->streamForResource('write');
        $this->mockPutObject('file.txt', $contents);
        $this->tosAdapter->writeStream('file.txt', $contents, new Config());
        $this->mockGetObject('file.txt', 'write');
        $this->assertSame('write', $this->tosAdapter->read('file.txt'));
    }

    /**
     * @return \Iterator<string[]>
     */
    public static function provideWriteStreamWithVisibilityCases(): \Iterator
    {
        yield [Visibility::PUBLIC];

        yield [Visibility::PRIVATE];
    }

    private function mockGetVisibility(string $path, string $visibility): void
    {
        $output = \Mockery::mock(GetObjectACLOutput::class);
        $grantee = (new Grantee());
        $grantee->setCanned(Enum::CannedAllUsers);
        $output->shouldReceive('getGrants')
            ->andReturn($visibility === Visibility::PUBLIC ? [new Grant($grantee, Enum::PermissionRead)] : []);
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
        $contents = $this->streamForResource('write');
        $this->mockPutObject('file.txt', $contents, $visibility);
        $this->tosAdapter->writeStream('file.txt', $contents, new Config([
            'visibility' => $visibility,
        ]));
        $this->mockGetVisibility('file.txt', $visibility);
        $this->assertSame($visibility, $this->tosAdapter->visibility('file.txt')['visibility']);
    }

    public function testWriteStreamWithExpires(): void
    {
        $contents = $this->streamForResource('write');
        $this->client->shouldReceive('putObject')
            ->withArgs([\Mockery::on(static function (PutObjectInput $argument) use ($contents): bool {
                if ($argument->getBucket() !== 'test') {
                    return false;
                }

                if ($argument->getKey() !== 'file.txt') {
                    return false;
                }

                if ($argument->getContent() !== $contents) {
                    return false;
                }

                if ($argument->getContentType() !== 'text/plain') {
                    return false;
                }

                return $argument->getExpires() === 20;
            }),
            ])->andReturn(null);
        rewind($contents);
        $this->tosAdapter->writeStream('file.txt', $contents, new Config([
            'Expires' => 20,
        ]));
        $this->mockGetObject('file.txt', 'write');
        $this->assertSame('write', $this->tosAdapter->read('file.txt'));
    }

    public function testWriteStreamWithMimetype(): void
    {
        $contents = $this->streamForResource('write');
        $this->client->shouldReceive('putObject')
            ->withArgs([\Mockery::on(static function (PutObjectInput $argument) use ($contents): bool {
                if ($argument->getBucket() !== 'test') {
                    return false;
                }

                if ($argument->getKey() !== 'file.txt') {
                    return false;
                }

                if ($argument->getContent() !== $contents) {
                    return false;
                }

                return $argument->getContentType() === 'image/png';
            }),
            ])->andReturn(null);
        rewind($contents);
        $this->tosAdapter->writeStream('file.txt', $contents, new Config([
            Constant::HeaderContentType => 'image/png',
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
        $this->assertSame('image/png', $this->tosAdapter->mimeType('file.txt')['mime_type']);
    }

    public function testDelete(): void
    {
        $contents = $this->streamForResource('write');
        $this->mockPutObject('file.txt', $contents);
        $this->tosAdapter->writeStream('file.txt', $contents, new Config());
        $this->mockGetMetadata('file.txt');
        $this->assertTrue($this->tosAdapter->fileExists('file.txt'));
        $this->client->shouldReceive('deleteObject')
            ->withArgs([\Mockery::on(static function (DeleteObjectInput $deleteObjectInput): bool {
                if ($deleteObjectInput->getBucket() !== 'test') {
                    return false;
                }

                return $deleteObjectInput->getKey() === 'file.txt';
            }),
            ])->andReturn(null);
        $this->tosAdapter->delete('file.txt');
        $this->mockGetEmptyMetadata('file.txt');
        $this->assertFalse($this->tosAdapter->fileExists('file.txt'));
    }

    public function testWrite(): void
    {
        $this->mockPutObject('file.txt', 'write');
        $this->tosAdapter->write('file.txt', 'write', new Config());
        $this->mockGetObject('file.txt', 'write');
        $this->assertSame('write', $this->tosAdapter->read('file.txt'));
    }

    public function testRead(): void
    {
        $this->mockGetObject('fixture/read.txt', 'read-test');
        $this->assertSame('read-test', $this->tosAdapter->read('fixture/read.txt'));
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

        $this->assertSame('read-test', stream_get_contents($this->tosAdapter->readStream('fixture/read.txt')));
    }

    public function testGetVisibility(): void
    {
        $this->mockGetVisibility('fixture/read.txt', Visibility::PRIVATE);
        $this->assertSame(Visibility::PRIVATE, $this->tosAdapter->visibility('fixture/read.txt')['visibility']);
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

    private function mockGetEmptyMetadata(string $path): void
    {
        $this->client->shouldReceive('headObject')
            ->once()
            ->withArgs([\Mockery::on(static function (HeadObjectInput $headObjectInput) use ($path): bool {
                if ($headObjectInput->getBucket() !== 'test') {
                    return false;
                }

                return $headObjectInput->getKey() === $path;
            }),
            ])->andThrow(new \Tos\Exception\TosServerException());
    }

    public function testListContents(): void
    {
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
        $this->client->shouldReceive('getObjectMeta')
            ->withArgs(['test', 'path/'])->andReturn([
                'ContentLength' => 0,
                'Date' => 'Mon, 31 May 2021 06:52:32 GMT',
                'RequestId' => '00000179C13207EF9217A7F5589D2DC6',
                'Id2' => '32AAAQAAEAABAAAQAAEAABAAAQAAEAABCSvXM+dHYwFYYJv2m9y5LibcMVibe3QN',
                'Reserved' => 'amazon, aws and amazon web services are trademarks or registered trademarks of Amazon Technologies, Inc',
                'Expiration' => '',
                'LastModified' => 'Mon, 31 May 2021 06:52:31 GMT',
                'ContentType' => 'binary/octet-stream',
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
        $this->assertNotEmpty(iterator_to_array($this->tosAdapter->listContents('path', false), false));
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
        $this->assertEmpty(iterator_to_array($this->tosAdapter->listContents('path1', false)));
        $this->mockPutObject('a/b/file.txt', 'test');
        $this->tosAdapter->write('a/b/file.txt', 'test', new Config());
        $output = \Mockery::mock(ListObjectsOutput::class);
        $output->shouldReceive('getNextMarker')
            ->andReturn('');
        $owner = null;
        $output->shouldReceive('getContents')
            ->andReturn([new ListedObject(
                'a/b/file.txt',
                1_622_474_604,
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
        $contents = iterator_to_array($this->tosAdapter->listContents('a', true));
        $this->assertContainsOnlyInstancesOf(StorageAttributes::class, $contents);
        $this->assertCount(2, $contents);

        /** @var \League\Flysystem\FileAttributes $file */
        $file = $contents[0];
        $this->assertInstanceOf(FileAttributes::class, $file);
        $this->assertSame('a/b/file.txt', $file->path());
        $this->assertSame(9, $file->fileSize());

        $this->assertNull($file->mimeType());
        $this->assertSame(1_622_474_604, $file->lastModified());
        $this->assertNull($file->visibility());
        $this->assertSame([
            Constant::HeaderStorageClass => 'STANDARD_IA',
            Constant::HeaderETag => 'd41d8cd98f00b204e9800998ecf8427e',
        ], $file->extraMetadata());

        /** @var \League\Flysystem\DirectoryAttributes $directory */
        $directory = $contents[1];
        $this->assertInstanceOf(DirectoryAttributes::class, $directory);
        $this->assertSame('a/b', $directory->path());
    }

    public function testGetSize(): void
    {
        $this->mockGetMetadata('fixture/read.txt');
        $this->assertSame(9, $this->tosAdapter->fileSize('fixture/read.txt')->fileSize());
    }

    public function testGetSizeError(): void
    {
        $this->mockGetEmptyMetadata('fixture/read.txt');
        $this->expectException(UnableToRetrieveMetadata::class);
        $this->assertSame(9, $this->tosAdapter->fileSize('fixture/read.txt')->fileSize());
    }

    public function testGetTimestamp(): void
    {
        $this->mockGetMetadata('fixture/read.txt');
        $this->assertSame(1_622_443_952, $this->tosAdapter->lastModified('fixture/read.txt')->lastModified());
    }

    public function testGetTimestampError(): void
    {
        $this->mockGetEmptyMetadata('fixture/read.txt');
        $this->expectException(UnableToRetrieveMetadata::class);
        $this->assertSame(1_622_443_952, $this->tosAdapter->lastModified('fixture/read.txt')->lastModified());
    }

    public function testGetMimetype(): void
    {
        $this->mockGetMetadata('fixture/read.txt');
        $this->assertSame('text/plain', $this->tosAdapter->mimeType('fixture/read.txt')->mimeType());
    }

    public function testGetMimetypeError(): void
    {
        $this->mockGetEmptyMetadata('fixture/read.txt');
        $this->expectException(UnableToRetrieveMetadata::class);
        $this->assertSame('text/plain', $this->tosAdapter->mimeType('fixture/read.txt')->mimeType());
    }

    public function testGetMetadataError(): void
    {
        $this->mockGetEmptyMetadata('fixture/');
        $this->expectException(UnableToRetrieveMetadata::class);
        $this->assertSame('text/plain', $this->tosAdapter->mimeType('fixture/')->mimeType());
    }

    public function testHas(): void
    {
        $this->mockGetMetadata('fixture/read.txt');
        $this->assertTrue($this->tosAdapter->fileExists('fixture/read.txt'));
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

    public function testDirectoryExists(): void
    {
        $output = \Mockery::mock(ListObjectsOutput::class);
        $output->shouldReceive('getNextMarker')
            ->andReturn('');
        $output->shouldReceive('getContents')
            ->andReturn([]);
        $output->shouldReceive('getCommonPrefixes')
            ->andReturn([]);
        $this->client->shouldReceive('listObjects')
            ->once()
            ->withArgs([\Mockery::on(static function (ListObjectsInput $listObjectsInput): bool {
                if ($listObjectsInput->getBucket() !== 'test') {
                    return false;
                }

                if ($listObjectsInput->getPrefix() !== 'fixture/exists-directory/') {
                    return false;
                }

                if ($listObjectsInput->getMaxKeys() !== 1) {
                    return false;
                }

                if ($listObjectsInput->getDelimiter() !== '/') {
                    return false;
                }

                return $listObjectsInput->getMarker() === '';
            }),
            ])->andReturn($output);
        $this->mockPutObject('fixture/exists-directory/', '', Visibility::PUBLIC);
        $this->assertFalse($this->tosAdapter->directoryExists('fixture/exists-directory'));
        $this->tosAdapter->createDirectory('fixture/exists-directory', new Config());

        $output = \Mockery::mock(ListObjectsOutput::class);
        $output->shouldReceive('getNextMarker')
            ->andReturn('');
        $output->shouldReceive('getContents')
            ->andReturn([new ListedObject('fixture/exists-directory/')]);
        $output->shouldReceive('getCommonPrefixes')
            ->andReturn([]);
        $this->client->shouldReceive('listObjects')
            ->once()
            ->withArgs([\Mockery::on(static function (ListObjectsInput $listObjectsInput): bool {
                if ($listObjectsInput->getBucket() !== 'test') {
                    return false;
                }

                if ($listObjectsInput->getPrefix() !== 'fixture/exists-directory/') {
                    return false;
                }

                if ($listObjectsInput->getMaxKeys() !== 1) {
                    return false;
                }

                if ($listObjectsInput->getDelimiter() !== '/') {
                    return false;
                }

                return $listObjectsInput->getMarker() === '';
            }),
            ])->andReturn($output);
        $this->assertTrue($this->tosAdapter->directoryExists('fixture/exists-directory'));
    }

    public function testMovingAFileWithVisibility(): void
    {
        $this->mockPutObject('source.txt', 'contents to be copied', Visibility::PUBLIC);
        $this->client->shouldReceive('copyObject')
            ->withArgs([
                \Mockery::on(static function (CopyObjectInput $copyObjectInput): bool {
                    if ($copyObjectInput->getBucket() !== 'test') {
                        return false;
                    }

                    if ($copyObjectInput->getKey() !== 'destination.txt') {
                        return false;
                    }

                    if ($copyObjectInput->getSrcBucket() !== 'test') {
                        return false;
                    }

                    if ($copyObjectInput->getACL() !== Enum::ACLPrivate) {
                        return false;
                    }

                    return $copyObjectInput->getSrcKey() === 'source.txt';
                }),
            ]);
        $this->client->shouldReceive('deleteObject')
            ->withArgs([\Mockery::on(static function (DeleteObjectInput $deleteObjectInput): bool {
                if ($deleteObjectInput->getBucket() !== 'test') {
                    return false;
                }

                return $deleteObjectInput->getKey() === 'source.txt';
            }),
            ]);
        $this->mockGetEmptyMetadata('source.txt');
        $this->mockGetMetadata('destination.txt');
        $this->mockGetVisibility('destination.txt', Visibility::PRIVATE);
        $this->mockGetObject('destination.txt', 'contents to be copied');
        $adapter = $this->tosAdapter;
        $adapter->write(
            'source.txt',
            'contents to be copied',
            new Config([
                Config::OPTION_VISIBILITY => Visibility::PUBLIC,
            ])
        );
        $adapter->move('source.txt', 'destination.txt', new Config([
            Config::OPTION_VISIBILITY => Visibility::PRIVATE,
        ]));
        $this->assertFalse(
            $adapter->fileExists('source.txt'),
            'After moving a file should no longer exist in the original location.'
        );
        $this->assertTrue(
            $adapter->fileExists('destination.txt'),
            'After moving, a file should be present at the new location.'
        );
        $this->assertSame(Visibility::PRIVATE, $adapter->visibility('destination.txt')->visibility());
        $this->assertSame('contents to be copied', $adapter->read('destination.txt'));
    }

    public function testCopyingAFileWithVisibility(): void
    {
        $this->mockPutObject('source.txt', 'contents to be copied', Visibility::PUBLIC);
        $this->client->shouldReceive('copyObject')
            ->withArgs([
                \Mockery::on(static function (CopyObjectInput $copyObjectInput): bool {
                    if ($copyObjectInput->getBucket() !== 'test') {
                        return false;
                    }

                    if ($copyObjectInput->getKey() !== 'destination.txt') {
                        return false;
                    }

                    if ($copyObjectInput->getSrcBucket() !== 'test') {
                        return false;
                    }

                    if ($copyObjectInput->getACL() !== Enum::ACLPrivate) {
                        return false;
                    }

                    return $copyObjectInput->getSrcKey() === 'source.txt';
                }),
            ]);
        $this->client->shouldReceive('deleteObject')
            ->withArgs([\Mockery::on(static function (DeleteObjectInput $deleteObjectInput): bool {
                if ($deleteObjectInput->getBucket() !== 'test') {
                    return false;
                }

                return $deleteObjectInput->getKey() === 'source.txt';
            }),
            ]);
        $this->mockGetMetadata('source.txt');
        $this->mockGetMetadata('destination.txt');
        $this->mockGetVisibility('destination.txt', Visibility::PRIVATE);
        $this->mockGetObject('destination.txt', 'contents to be copied');
        $adapter = $this->tosAdapter;
        $adapter->write(
            'source.txt',
            'contents to be copied',
            new Config([
                Config::OPTION_VISIBILITY => Visibility::PUBLIC,
            ])
        );

        $adapter->copy('source.txt', 'destination.txt', new Config([
            Config::OPTION_VISIBILITY => Visibility::PRIVATE,
        ]));

        $this->assertTrue($adapter->fileExists('source.txt'));
        $this->assertTrue($adapter->fileExists('destination.txt'));
        $this->assertSame(Visibility::PRIVATE, $adapter->visibility('destination.txt')->visibility());
        $this->assertSame('contents to be copied', $adapter->read('destination.txt'));
    }
}
