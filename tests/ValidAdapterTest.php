<?php

declare(strict_types=1);

namespace Zing\Flysystem\Tos\Tests;

use League\Flysystem\Config;
use League\Flysystem\DirectoryAttributes;
use League\Flysystem\FileAttributes;
use League\Flysystem\StorageAttributes;
use League\Flysystem\Visibility;
use Tos\Model\Constant;
use Tos\TosClient;
use Zing\Flysystem\Tos\TosAdapter;

/**
 * @internal
 */
final class ValidAdapterTest extends TestCase
{
    private TosAdapter $tosAdapter;

    private function getKey(): string
    {
        return (string) getenv('TOS_KEY') ?: '';
    }

    private function getSecret(): string
    {
        return (string) getenv('TOS_SECRET') ?: '';
    }

    private function getBucket(): string
    {
        return (string) getenv('TOS_BUCKET') ?: '';
    }

    private function getEndpoint(): string
    {
        return (string) getenv('TOS_ENDPOINT') ?: 'tos-cn-shanghai.volces.com';
    }

    protected function setUp(): void
    {
        if ((string) getenv('MOCK') !== 'false') {
            $this->markTestSkipped('Mock tests enabled');
        }

        parent::setUp();

        $config = [
            'key' => $this->getKey(),
            'secret' => $this->getSecret(),
            'bucket' => $this->getBucket(),
            'endpoint' => $this->getEndpoint(),
            'path_style' => '',
            'region' => '',
        ];

        $this->tosAdapter = new TosAdapter(new TosClient(
            'cn-shanghai',
            $config['key'],
            $config['secret'],
            $config['endpoint']
        ), $this->getBucket(), '');
        $this->tosAdapter->write('fixture/read.txt', 'read-test', new Config([
            Config::OPTION_VISIBILITY => Visibility::PUBLIC,
        ]));
    }

    protected function tearDown(): void
    {
        parent::tearDown();

        $this->tosAdapter->deleteDirectory('fixture');
    }

    public function testCopy(): void
    {
        $this->tosAdapter->write('fixture/file.txt', 'write', new Config());
        $this->tosAdapter->copy('fixture/file.txt', 'fixture/copy.txt', new Config());
        $this->assertSame('write', $this->tosAdapter->read('fixture/copy.txt'));
    }

    public function testCopyWithoutRetainVisibility(): void
    {
        $this->tosAdapter->write('fixture/file.txt', 'write', new Config());
        $this->tosAdapter->copy('fixture/file.txt', 'fixture/copy.txt', new Config([
            'retain_visibility' => false,
        ]));
        $this->assertSame(Visibility::PRIVATE, $this->tosAdapter->visibility('fixture/copy.txt')->visibility());
    }

    public function testCreateDir(): void
    {
        $this->tosAdapter->createDirectory('fixture/path', new Config());
        $this->assertTrue($this->tosAdapter->directoryExists('fixture/path'));
        $this->assertSame([], iterator_to_array($this->tosAdapter->listContents('fixture/path', false)));
        $this->assertSame([], iterator_to_array($this->tosAdapter->listContents('fixture/path/', false)));
        $this->tosAdapter->write('fixture/path1/file.txt', 'test', new Config());
        $contents = iterator_to_array($this->tosAdapter->listContents('fixture/path1', false));
        $this->assertCount(1, $contents);
        $file = $contents[0];
        $this->assertSame('fixture/path1/file.txt', $file['path']);
        $this->tosAdapter->deleteDirectory('fixture/path');
        $this->assertFalse($this->tosAdapter->directoryExists('fixture/path'));
        $this->tosAdapter->deleteDirectory('fixture/path1');
        $this->assertFalse($this->tosAdapter->directoryExists('fixture/path1'));
    }

    public function testDirectoryExists(): void
    {
        $this->assertFalse($this->tosAdapter->directoryExists('fixture/exists-directory'));
        $this->tosAdapter->createDirectory('fixture/exists-directory', new Config());
        $this->assertTrue($this->tosAdapter->directoryExists('fixture/exists-directory'));
    }

    public function testSetVisibility(): void
    {
        $this->tosAdapter->write('fixture/file.txt', 'write', new Config([
            'visibility' => Visibility::PRIVATE,
        ]));
        $this->assertSame(Visibility::PRIVATE, $this->tosAdapter->visibility('fixture/file.txt')['visibility']);
        $this->tosAdapter->setVisibility('fixture/file.txt', Visibility::PUBLIC);
        $this->assertSame(Visibility::PUBLIC, $this->tosAdapter->visibility('fixture/file.txt')['visibility']);
    }

    public function testRename(): void
    {
        $this->tosAdapter->write('fixture/from.txt', 'write', new Config());
        $this->assertTrue($this->tosAdapter->fileExists('fixture/from.txt'));
        $this->assertFalse($this->tosAdapter->fileExists('fixture/to.txt'));
        $this->tosAdapter->move('fixture/from.txt', 'fixture/to.txt', new Config());
        $this->assertFalse($this->tosAdapter->fileExists('fixture/from.txt'));
        $this->assertSame('write', $this->tosAdapter->read('fixture/to.txt'));
        $this->tosAdapter->delete('fixture/to.txt');
    }

    public function testDeleteDir(): void
    {
        $this->tosAdapter->deleteDirectory('fixture');
        $this->assertFalse($this->tosAdapter->directoryExists('fixture'));
    }

    public function testWriteStream(): void
    {
        $this->tosAdapter->writeStream('fixture/file.txt', $this->streamForResource('write'), new Config());
        $this->assertSame('write', $this->tosAdapter->read('fixture/file.txt'));
    }

    /**
     * @return \Iterator<string[]>
     */
    public static function provideVisibilities(): \Iterator
    {
        yield [Visibility::PUBLIC];

        yield [Visibility::PRIVATE];
    }

    /**
     * @dataProvider provideVisibilities
     */
    public function testWriteStreamWithVisibility(string $visibility): void
    {
        $this->tosAdapter->writeStream('fixture/file.txt', $this->streamForResource('write'), new Config([
            'visibility' => $visibility,
        ]));
        $this->assertSame($visibility, $this->tosAdapter->visibility('fixture/file.txt')['visibility']);
    }

    public function testWriteStreamWithExpires(): void
    {
        $this->tosAdapter->writeStream('fixture/file.txt', $this->streamForResource('write'), new Config([
            'Expires' => 20,
        ]));
        $this->assertSame('write', $this->tosAdapter->read('fixture/file.txt'));
    }

    public function testWriteStreamWithMimetype(): void
    {
        $this->tosAdapter->writeStream('fixture/file.txt', $this->streamForResource('write'), new Config([
            Constant::HeaderContentType => 'image/png',
        ]));
        $this->assertSame('image/png', $this->tosAdapter->mimeType('fixture/file.txt')['mime_type']);
    }

    public function testDelete(): void
    {
        $this->tosAdapter->writeStream('fixture/file.txt', $this->streamForResource('test'), new Config());
        $this->assertTrue($this->tosAdapter->fileExists('fixture/file.txt'));
        $this->tosAdapter->delete('fixture/file.txt');
        $this->assertFalse($this->tosAdapter->fileExists('fixture/file.txt'));
    }

    public function testWrite(): void
    {
        $this->tosAdapter->write('fixture/file.txt', 'write', new Config());
        $this->assertSame('write', $this->tosAdapter->read('fixture/file.txt'));
    }

    public function testRead(): void
    {
        $this->assertSame('read-test', $this->tosAdapter->read('fixture/read.txt'));
    }

    public function testReadStream(): void
    {
        $this->assertSame('read-test', stream_get_contents($this->tosAdapter->readStream('fixture/read.txt')));
    }

    public function testGetVisibility(): void
    {
        $this->assertSame(Visibility::PUBLIC, $this->tosAdapter->visibility('fixture/read.txt')->visibility());
    }

    public function testListContents(): void
    {
        $this->assertNotEmpty(iterator_to_array($this->tosAdapter->listContents('fixture', false)));
        $this->assertEmpty(iterator_to_array($this->tosAdapter->listContents('path1', false)));
        $this->tosAdapter->createDirectory('fixture/path/dir', new Config());
        $this->tosAdapter->write('fixture/path/dir/file.txt', 'test', new Config());

        /** @var \League\Flysystem\StorageAttributes[] $contents */
        $contents = iterator_to_array($this->tosAdapter->listContents('fixture/path', true));
        $this->assertContainsOnlyInstancesOf(StorageAttributes::class, $contents);
        $this->assertCount(2, $contents);

        /** @var \League\Flysystem\FileAttributes $file */
        /** @var \League\Flysystem\DirectoryAttributes $directory */
        [$file, $directory] = $contents[0]->isFile() ? [$contents[0], $contents[1]] : [$contents[1], $contents[0]];
        $this->assertInstanceOf(FileAttributes::class, $file);
        $this->assertSame('fixture/path/dir/file.txt', $file->path());
        $this->assertSame(4, $file->fileSize());

        $this->assertNull($file->mimeType());
        $this->assertNotNull($file->lastModified());
        $this->assertNull($file->visibility());
        $this->assertIsArray($file->extraMetadata());
        $this->assertInstanceOf(DirectoryAttributes::class, $directory);
        $this->assertSame('fixture/path/dir', $directory->path());
    }

    public function testGetSize(): void
    {
        $this->assertSame(9, $this->tosAdapter->fileSize('fixture/read.txt')->fileSize());
    }

    public function testGetTimestamp(): void
    {
        $this->assertGreaterThan(time() - 10, $this->tosAdapter->lastModified('fixture/read.txt')->lastModified());
    }

    public function testGetMimetype(): void
    {
        $this->assertSame('text/plain', $this->tosAdapter->mimeType('fixture/read.txt')->mimeType());
    }

    public function testHas(): void
    {
        $this->assertTrue($this->tosAdapter->fileExists('fixture/read.txt'));
    }

    public function testGetTemporaryUrl(): void
    {
        $this->assertSame(
            'read-test',
            file_get_contents($this->tosAdapter->getTemporaryUrl('fixture/read.txt', 10, []))
        );
    }

    public function testImage(): void
    {
        $contents = file_get_contents('https://avatars.githubusercontent.com/u/26657141');
        if ($contents === false) {
            $this->markTestSkipped('Require image contents');
        }

        $this->tosAdapter->write('fixture/image.png', $contents, new Config());

        /** @var array{int, int} $info */
        $info = getimagesize($this->tosAdapter->getTemporaryUrl('fixture/image.png', 10, [
            'x-tos-process' => 'image/crop,w_200,h_100',
        ]));

        $this->assertSame(200, $info[0]);
        $this->assertSame(100, $info[1]);
    }

    /**
     * @dataProvider provideVisibilities
     */
    public function testCopyWithVisibility(string $visibility): void
    {
        $this->tosAdapter->write('fixture/private.txt', 'private', new Config([
            Config::OPTION_VISIBILITY => $visibility,
        ]));
        $this->tosAdapter->copy('fixture/private.txt', 'fixture/copied-private.txt', new Config());
        $this->assertSame($visibility, $this->tosAdapter->visibility('fixture/copied-private.txt')->visibility());
    }

    public function testMovingAFileWithVisibility(): void
    {
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
