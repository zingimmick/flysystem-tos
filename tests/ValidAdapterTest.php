<?php

declare(strict_types=1);

namespace Zing\Flysystem\Tos\Tests;

use League\Flysystem\AdapterInterface;
use League\Flysystem\Config;
use Tos\Model\Constant;
use Tos\TosClient;
use Zing\Flysystem\Tos\TosAdapter;

/**
 * @internal
 */
final class ValidAdapterTest extends TestCase
{
    /**
     * @var \Zing\Flysystem\Tos\TosAdapter
     */
    private $tosAdapter;

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
        return (string) getenv('TOS_ENDPOINT') ?: '';
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
        ), $this->getBucket(), '', [
            'default_visibility' => AdapterInterface::VISIBILITY_PUBLIC,
        ]);
        $this->tosAdapter->write('fixture/read.txt', 'read-test', new Config(
            [
                'visibility' => AdapterInterface::VISIBILITY_PRIVATE,
            ]
        ));
    }

    protected function tearDown(): void
    {
        parent::tearDown();

        $this->tosAdapter->deleteDir('fixture');
    }

    public function testUpdate(): void
    {
        $this->tosAdapter->update('fixture/file.txt', 'update', new Config());
        $this->assertSame('update', $this->tosAdapter->read('fixture/file.txt')['contents']);
    }

    public function testUpdateStream(): void
    {
        $this->tosAdapter->write('fixture/file.txt', 'write', new Config());
        $this->tosAdapter->updateStream('fixture/file.txt', $this->streamFor('update')->detach(), new Config());
        $this->assertSame('update', $this->tosAdapter->read('fixture/file.txt')['contents']);
    }

    public function testCopy(): void
    {
        $this->tosAdapter->write('fixture/file.txt', 'write', new Config());
        $this->tosAdapter->copy('fixture/file.txt', 'fixture/copy.txt');
        $this->assertSame('write', $this->tosAdapter->read('fixture/copy.txt')['contents']);
    }

    public function testCreateDir(): void
    {
        $this->tosAdapter->createDir('fixture/path', new Config());
        $this->assertSame([], $this->tosAdapter->listContents('fixture/path'));
        $this->assertSame([], $this->tosAdapter->listContents('fixture/path/'));
        $this->tosAdapter->write('fixture/path1/file.txt', 'test', new Config());
        $contents = $this->tosAdapter->listContents('fixture/path1');
        $this->assertCount(1, $contents);
        $file = $contents[0];
        $this->assertSame('fixture/path1/file.txt', $file['path']);
    }

    public function testSetVisibility(): void
    {
        $this->tosAdapter->write('fixture/file.txt', 'write', new Config([
            'visibility' => AdapterInterface::VISIBILITY_PRIVATE,
        ]));
        $this->assertSame(
            AdapterInterface::VISIBILITY_PRIVATE,
            $this->tosAdapter->getVisibility('fixture/file.txt')['visibility']
        );
        $this->tosAdapter->setVisibility('fixture/file.txt', AdapterInterface::VISIBILITY_PUBLIC);
        $this->assertSame(
            AdapterInterface::VISIBILITY_PUBLIC,
            $this->tosAdapter->getVisibility('fixture/file.txt')['visibility']
        );
    }

    public function testRename(): void
    {
        $this->tosAdapter->write('fixture/from.txt', 'write', new Config());
        $this->assertTrue($this->tosAdapter->has('fixture/from.txt'));
        $this->assertFalse($this->tosAdapter->has('fixture/to.txt'));
        $this->tosAdapter->rename('fixture/from.txt', 'fixture/to.txt');
        $this->assertFalse($this->tosAdapter->has('fixture/from.txt'));
        $this->assertSame('write', $this->tosAdapter->read('fixture/to.txt')['contents']);
        $this->tosAdapter->delete('fixture/to.txt');
    }

    public function testDeleteDir(): void
    {
        $this->assertTrue($this->tosAdapter->deleteDir('fixture'));
        $this->assertFalse($this->tosAdapter->has('fixture'));
    }

    public function testWriteStream(): void
    {
        $this->tosAdapter->writeStream('fixture/file.txt', $this->streamFor('write')->detach(), new Config());
        $this->assertSame('write', $this->tosAdapter->read('fixture/file.txt')['contents']);
    }

    /**
     * @return \Iterator<string[]>
     */
    public function provideWriteStreamWithVisibilityCases(): \Iterator
    {
        yield [AdapterInterface::VISIBILITY_PUBLIC];

        yield [AdapterInterface::VISIBILITY_PRIVATE];
    }

    /**
     * @dataProvider provideWriteStreamWithVisibilityCases
     */
    public function testWriteStreamWithVisibility(string $visibility): void
    {
        $this->tosAdapter->writeStream('fixture/file.txt', $this->streamFor('write')->detach(), new Config([
            'visibility' => $visibility,
        ]));
        $this->assertSame($visibility, $this->tosAdapter->getVisibility('fixture/file.txt')['visibility']);
    }

    public function testWriteStreamWithExpires(): void
    {
        $this->tosAdapter->writeStream('fixture/file.txt', $this->streamFor('write')->detach(), new Config([
            'Expires' => 20,
        ]));
        $this->assertSame('write', $this->tosAdapter->read('fixture/file.txt')['contents']);
    }

    public function testWriteStreamWithMimetype(): void
    {
        $this->tosAdapter->writeStream('fixture/file.txt', $this->streamFor('write')->detach(), new Config([
            Constant::HeaderContentType => 'image/png',
        ]));
        $this->assertSame('image/png', $this->tosAdapter->getMimetype('fixture/file.txt')['mimetype']);
    }

    public function testDelete(): void
    {
        $this->tosAdapter->writeStream('fixture/file.txt', $this->streamFor('test')->detach(), new Config());
        $this->assertTrue($this->tosAdapter->has('fixture/file.txt'));
        $this->tosAdapter->delete('fixture/file.txt');
        $this->assertFalse($this->tosAdapter->has('fixture/file.txt'));
    }

    public function testWrite(): void
    {
        $this->tosAdapter->write('fixture/file.txt', 'write', new Config());
        $this->assertSame('write', $this->tosAdapter->read('fixture/file.txt')['contents']);
    }

    public function testRead(): void
    {
        $this->assertSame('read-test', $this->tosAdapter->read('fixture/read.txt')['contents']);
    }

    public function testReadStream(): void
    {
        $this->assertSame(
            'read-test',
            stream_get_contents($this->tosAdapter->readStream('fixture/read.txt')['stream'])
        );
    }

    public function testGetVisibility(): void
    {
        $this->assertSame(
            AdapterInterface::VISIBILITY_PRIVATE,
            $this->tosAdapter->getVisibility('fixture/read.txt')['visibility']
        );
    }

    public function testGetMetadata(): void
    {
        $this->assertIsArray($this->tosAdapter->getMetadata('fixture/read.txt'));
    }

    public function testListContents(): void
    {
        $this->assertNotEmpty($this->tosAdapter->listContents('fixture'));
        $this->assertEmpty($this->tosAdapter->listContents('path1'));
        $this->tosAdapter->write('fixture/path/file.txt', 'test', new Config());
        $this->tosAdapter->listContents('a', true);
    }

    public function testGetSize(): void
    {
        $this->assertSame(9, $this->tosAdapter->getSize('fixture/read.txt')['size']);
    }

    public function testGetTimestamp(): void
    {
        $this->assertGreaterThan(time() - 10, $this->tosAdapter->getTimestamp('fixture/read.txt')['timestamp']);
    }

    public function testGetMimetype(): void
    {
        $this->assertSame('text/plain', $this->tosAdapter->getMimetype('fixture/read.txt')['mimetype']);
    }

    public function testHas(): void
    {
        $this->assertTrue($this->tosAdapter->has('fixture/read.txt'));
    }

    public function testSignUrl(): void
    {
        $this->assertSame('read-test', file_get_contents($this->tosAdapter->signUrl('fixture/read.txt', 10, [])));
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
        $this->tosAdapter->write(
            'fixture/image.png',
            file_get_contents('https://avatars.githubusercontent.com/u/26657141'),
            new Config()
        );
        $info = getimagesize($this->tosAdapter->signUrl('fixture/image.png', 10, [
            'x-tos-process' => 'image/crop,w_200,h_100',
        ]));
        $this->assertSame(200, $info[0]);
        $this->assertSame(100, $info[1]);
    }

    public function testForceMimetype(): void
    {
        $this->tosAdapter->write('fixture/file.txt', 'test', new Config([
            'mimetype' => 'image/png',
        ]));
        $this->assertSame('image/png', $this->tosAdapter->getMimetype('fixture/file.txt')['mimetype']);
        $this->tosAdapter->write('fixture/file2.txt', 'test', new Config([
            Constant::HeaderContentType => 'image/png',
        ]));
        $this->assertSame('image/png', $this->tosAdapter->getMimetype('fixture/file2.txt')['mimetype']);
    }
}
