<?php

declare(strict_types=1);

namespace Zing\Flysystem\Tos\Tests;

use League\Flysystem\AdapterTestUtilities\FilesystemAdapterTestCase;
use League\Flysystem\Config;
use League\Flysystem\FilesystemAdapter;
use Tos\TosClient;
use Zing\Flysystem\Tos\TosAdapter;

/**
 * @internal
 */
final class TosAdapterTest extends FilesystemAdapterTestCase
{
    protected static function createFilesystemAdapter(): FilesystemAdapter
    {
        $config = [
            'key' => (string) getenv('TOS_KEY') ?: '',
            'secret' => (string) getenv('TOS_SECRET') ?: '',
            'bucket' => (string) getenv('TOS_BUCKET') ?: '',
            'endpoint' => (string) getenv('TOS_ENDPOINT') ?: 'tos-cn-shanghai.volces.com',
            'path_style' => '',
            'region' => '',
        ];

        return new TosAdapter(new TosClient(
            'cn-shanghai',
            $config['key'],
            $config['secret'],
            $config['endpoint']
        ), $config['bucket'] ?: '', 'github-test', null, null, [
            'endpoint' => $config['endpoint'],
        ]);
    }

    private FilesystemAdapter $filesystemAdapter;

    protected function setUp(): void
    {
        if ((string) getenv('MOCK') !== 'false') {
            $this->markTestSkipped('Mock tests enabled');
        }

        $this->filesystemAdapter = self::createFilesystemAdapter();

        parent::setUp();
    }

    public function adapter(): FilesystemAdapter
    {
        return $this->filesystemAdapter;
    }

    protected function tearDown(): void
    {
        parent::tearDown();

        $adapter = $this->adapter();
        $adapter->deleteDirectory('/');

        /** @var \League\Flysystem\StorageAttributes[] $listing */
        $listing = $adapter->listContents('', false);

        foreach ($listing as $singleListing) {
            if ($singleListing->isFile()) {
                $adapter->delete($singleListing->path());
            } else {
                $adapter->deleteDirectory($singleListing->path());
            }
        }
    }

    /**
     * @test
     */
    public function fetching_unknown_mime_type_of_a_file(): void
    {
        $this->adapter()
            ->write('unknown-mime-type.md5', '', new Config());

        $this->runScenario(function (): void {
            $this->assertSame('binary/octet-stream', $this->adapter()
                ->mimeType('unknown-mime-type.md5')
                ->mimeType());
        });
    }
}
