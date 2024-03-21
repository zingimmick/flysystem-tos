<?php

declare(strict_types=1);

namespace Zing\Flysystem\Tos;

use GuzzleHttp\Psr7\Uri;
use League\Flysystem\Adapter\AbstractAdapter;
use League\Flysystem\AdapterInterface;
use League\Flysystem\Config;
use League\Flysystem\Util;
use Psr\Http\Message\UriInterface;
use Tos\Exception\TosServerException;
use Tos\Model\Constant;
use Tos\Model\CopyObjectInput;
use Tos\Model\DeleteMultiObjectsInput;
use Tos\Model\DeleteObjectInput;
use Tos\Model\Enum;
use Tos\Model\GetObjectACLInput;
use Tos\Model\GetObjectACLOutput;
use Tos\Model\GetObjectInput;
use Tos\Model\HeadObjectInput;
use Tos\Model\ListObjectsInput;
use Tos\Model\ObjectTobeDeleted;
use Tos\Model\PreSignedURLInput;
use Tos\Model\PutObjectACLInput;
use Tos\Model\PutObjectInput;
use Tos\TosClient;

class TosAdapter extends AbstractAdapter
{
    /**
     * @var string
     */
    private const DELIMITER = '/';

    /**
     * @var int
     */
    private const MAX_KEYS = 1000;

    /**
     * @var string[]
     */
    private const AVAILABLE_OPTIONS = [Constant::HeaderAcl, Constant::HeaderContentType, Constant::HeaderExpires];

    /**
     * @var string
     */
    protected $bucket;

    /**
     * @var array{url?: string, temporary_url?: string, endpoint?: string, bucket_endpoint?: bool}
     */
    protected $options = [];

    /**
     * @var \TOS\TosClient
     */
    protected $client;

    /**
     * @param array{url?: string, temporary_url?: string, endpoint?: string, bucket_endpoint?: bool} $options
     */
    public function __construct(TosClient $client, string $bucket, string $prefix = '', array $options = [])
    {
        $this->client = $client;
        $this->bucket = $bucket;
        $this->setPathPrefix($prefix);
        $this->options = $options;
    }

    public function getBucket(): string
    {
        return $this->bucket;
    }

    public function setBucket(string $bucket): void
    {
        $this->bucket = $bucket;
    }

    public function getClient(): TosClient
    {
        return $this->client;
    }

    public function kernel(): TosClient
    {
        return $this->getClient();
    }

    public function write($path, $contents, Config $config): bool
    {
        return $this->upload($path, $contents, $config);
    }

    /**
     * @param mixed $path
     * @param resource $resource
     */
    public function writeStream($path, $resource, Config $config): bool
    {
        return $this->upload($path, $resource, $config);
    }

    private function upload(string $path, $contents, Config $config): bool
    {
        $options = $this->createOptionsFromConfig($config);
        $putObjectInput = new PutObjectInput($this->bucket, $this->applyPathPrefix($path), $contents);
        if (! isset($options[Constant::HeaderAcl])) {
            /** @var string|null $visibility */
            $visibility = $config->get('visibility');
            if ($visibility !== null) {
                $putObjectInput->setACL(
                    $options[Constant::HeaderAcl] ?? ($visibility === self::VISIBILITY_PUBLIC ? Enum::ACLPublicRead : Enum::ACLPrivate)
                );
            }
        }

        $shouldDetermineMimetype = $contents !== '' && ! \array_key_exists(Constant::HeaderContentType, $options);

        if ($shouldDetermineMimetype) {
            $mimeType = Util::guessMimeType($path, $contents);
            if ($mimeType) {
                $putObjectInput->setContentType($mimeType);
            }
        }

        if (isset($options[Constant::HeaderContentType])) {
            $putObjectInput->setContentType($options[Constant::HeaderContentType]);
        }

        if (isset($options[Constant::HeaderExpires])) {
            $putObjectInput->setExpires($options[Constant::HeaderExpires]);
        }

        try {
            $this->client->putObject($putObjectInput);
        } catch (TosServerException $tosServerException) {
            return false;
        }

        return true;
    }

    public function rename($path, $newpath): bool
    {
        if (! $this->copy($path, $newpath)) {
            return false;
        }

        return $this->delete($path);
    }

    public function delete($path): bool
    {
        try {
            $deleteObjectInput = new DeleteObjectInput($this->bucket, $this->applyPathPrefix($path));
            $this->client->deleteObject($deleteObjectInput);
        } catch (TosServerException $tosServerException) {
            return false;
        }

        return ! $this->has($path);
    }

    public function copy($path, $newpath): bool
    {
        try {
            $copyObjectInput = new CopyObjectInput(
                $this->bucket,
                $this->applyPathPrefix($newpath),
                $this->bucket,
                $this->applyPathPrefix($path)
            );
            $this->client->copyObject($copyObjectInput);
        } catch (TosServerException $tosServerException) {
            return false;
        }

        return true;
    }

    /**
     * @param mixed $path
     * @param mixed $visibility
     *
     * @return array|false
     */
    public function setVisibility($path, $visibility)
    {
        try {
            $putObjectACLInput = new PutObjectACLInput(
                $this->bucket,
                $this->applyPathPrefix($path),
                $this->visibilityToAcl($visibility)
            );
            $this->client->putObjectAcl($putObjectACLInput);
        } catch (TosServerException $tosServerException) {
            return false;
        }

        return [
            'path' => $path,
            'visibility' => $visibility,
        ];
    }

    /**
     * @param mixed $path
     *
     * @return array|false
     */
    public function read($path)
    {
        try {
            $getObjectInput = new GetObjectInput($this->bucket, $this->applyPathPrefix($path));
            $content = $this->client->getObject($getObjectInput)
                ->getContent();
            if ($content === null) {
                return false;
            }

            $contents = $content->getContents();

            return [
                'path' => $path,
                'contents' => $contents,
            ];
        } catch (TosServerException $tosServerException) {
            return false;
        }
    }

    /**
     * @param mixed $path
     *
     * @return array|false
     */
    public function readStream($path)
    {
        try {
            $getObjectInput = new GetObjectInput($this->bucket, $this->applyPathPrefix($path));
            $getObjectInput->setStreamMode(false);
            $content = $this->client->getObject($getObjectInput)
                ->getContent();
            if ($content === null) {
                return false;
            }

            $stream = $content->detach();

            return [
                'path' => $path,
                'stream' => $stream,
            ];
        } catch (TosServerException $tosServerException) {
            return false;
        }
    }

    /**
     * @param mixed $directory
     * @param mixed $recursive
     *
     * @return array<int, mixed[]>
     */
    public function listContents($directory = '', $recursive = false): array
    {
        $directory = rtrim($directory, '/');
        $result = $this->listDirObjects($directory, $recursive);
        $list = [];
        foreach ($result['objects'] as $files) {
            $path = $this->removePathPrefix(rtrim((string) ($files['key'] ?? $files['prefix']), '/'));
            if ($path === $directory) {
                continue;
            }

            $list[] = $this->mapObjectMetadata($files);
        }

        foreach ($result['prefix'] as $dir) {
            $list[] = [
                'type' => 'dir',
                'path' => $this->removePathPrefix(rtrim($dir, '/')),
            ];
        }

        return $list;
    }

    /**
     * @param mixed $path
     *
     * @return array|false
     */
    public function getMetadata($path)
    {
        try {
            $headObjectInput = new HeadObjectInput($this->bucket, $this->applyPathPrefix($path));
            $metadata = $this->client->headObject($headObjectInput);
        } catch (TosServerException $tosServerException) {
            return false;
        }

        return $this->mapObjectMetadata([
            'key' => $headObjectInput->getKey(),
            'last-modified' => $metadata->getLastModified(),
            'content-type' => $metadata->getContentType(),
            'size' => $metadata->getContentLength(),
            Constant::HeaderETag => $metadata->getETag(),
            Constant::HeaderStorageClass => $metadata->getStorageClass(),
        ], $path);
    }

    private function mapObjectMetadata(array $metadata, ?string $path = null): array
    {
        if ($path === null) {
            $path = $this->removePathPrefix((string) ($metadata['key'] ?? $metadata['prefix']));
        }

        if (substr($path, -1) === '/') {
            return [
                'type' => 'dir',
                'path' => rtrim($path, '/'),
            ];
        }

        return [
            'type' => 'file',
            'mimetype' => $metadata['content-type'] ?? null,
            'path' => $path,
            'timestamp' => $metadata['last-modified'] ?? null,
            'size' => isset($metadata['content-length']) ? (int) $metadata['content-length'] : ($metadata['size'] ?? null),
        ];
    }

    /**
     * File list core method.
     *
     * @return array{prefix: array<string>, objects: array<array{key?: string, prefix: ?string, content-length?: string, size?: int, last-modified?: string, content-type?: string}>}
     */
    public function listDirObjects(string $dirname = '', bool $recursive = false): array
    {
        $prefix = trim($this->applyPathPrefix($dirname), '/');
        $prefix = $prefix === '' ? '' : $prefix . '/';

        $nextMarker = '';

        $result = [];

        while (true) {
            $input = new ListObjectsInput($this->bucket, self::MAX_KEYS, $prefix, $nextMarker);
            $input->setDelimiter($recursive ? '' : self::DELIMITER);
            $model = $this->client->listObjects($input);
            $nextMarker = $model->getNextMarker();
            $objects = $model->getContents();
            $prefixes = $model->getCommonPrefixes();
            $result = $this->processObjects($result, $objects, $dirname);

            $result = $this->processPrefixes($result, $prefixes);
            if ($nextMarker === '') {
                break;
            }
        }

        return $result;
    }

    /**
     * @param array{prefix?: array<string>, objects?: array<array{key?: string, prefix: string|null, content-length?: string, size?: int, last-modified?: string, content-type?: string}>} $result
     * @param array<\Tos\Model\ListedObject>|null $objects
     *
     * @return array{prefix?: array<string>, objects: array<array{key?: string, prefix: string|null, content-length?: string, size?: int, last-modified?: string, content-type?: string}>}
     */
    private function processObjects(array $result, ?array $objects, string $dirname): array
    {
        $result['objects'] = [];
        if ($objects !== null && $objects !== []) {
            foreach ($objects as $object) {
                $result['objects'][] = [
                    'prefix' => $dirname,
                    'key' => $object->getKey(),
                    'last-modified' => $object->getLastModified(),
                    'size' => $object->getSize(),
                    Constant::HeaderETag => $object->getETag(),
                    Constant::HeaderStorageClass => $object->getStorageClass(),
                ];
            }
        } else {
            $result['objects'] = [];
        }

        return $result;
    }

    /**
     * @param array{prefix?: array<string>, objects: array<array{key?: string, prefix: string|null, content-length?: string, size?: int, last-modified?: string, content-type?: string}>} $result
     * @param array<\Tos\Model\ListedCommonPrefix>|null $prefixes
     *
     * @return array{prefix: array<string>, objects: array<array{key?: string, prefix: string|null, content-length?: string, size?: int, last-modified?: string, content-type?: string}>}
     */
    private function processPrefixes(array $result, ?array $prefixes): array
    {
        if ($prefixes !== null && $prefixes !== []) {
            foreach ($prefixes as $prefix) {
                $result['prefix'][] = $prefix->getPrefix();
            }
        } else {
            $result['prefix'] = [];
        }

        return $result;
    }

    /**
     * Get options from the config.
     *
     * @return array<string, mixed>
     */
    protected function createOptionsFromConfig(Config $config): array
    {
        $options = $this->options;
        $mimeType = $config->get('mimetype');
        if ($mimeType) {
            $options[Constant::HeaderContentType] = $mimeType;
        }

        foreach (self::AVAILABLE_OPTIONS as $option) {
            $value = $config->get($option, '__NOT_SET__');

            if ($value !== '__NOT_SET__') {
                $options[$option] = $value;
            }
        }

        return $options;
    }

    /**
     * Get the URL for the file at the given path.
     */
    public function getUrl(string $path): string
    {
        if (isset($this->options['url'])) {
            return $this->concatPathToUrl($this->options['url'], $this->applyPathPrefix($path));
        }

        return $this->concatPathToUrl($this->normalizeHost(), $this->applyPathPrefix($path));
    }

    protected function normalizeHost(): string
    {
        if (! isset($this->options['endpoint'])) {
            throw UnableToGetUrl::missingOption('endpoint');
        }

        $endpoint = $this->options['endpoint'];
        if (strpos($endpoint, 'http') !== 0) {
            $endpoint = 'https://' . $endpoint;
        }

        /** @var array{scheme: string, host: string} $url */
        $url = parse_url($endpoint);
        $domain = $url['host'];
        if (! ($this->options['bucket_endpoint'] ?? false)) {
            $domain = $this->bucket . '.' . $domain;
        }

        $domain = sprintf('%s://%s', $url['scheme'], $domain);

        return rtrim($domain, '/') . '/';
    }

    /**
     * Get a signed URL for the file at the given path.
     *
     * @param \DateTimeInterface|int $expiration
     * @param array<string, mixed> $options
     *
     * @return string
     */
    public function signUrl(string $path, $expiration, array $options = [], string $method = 'GET')
    {
        $expires = $expiration instanceof \DateTimeInterface ? $expiration->getTimestamp() - time() : $expiration;

        $preSignedURLInput = new PreSignedURLInput(
            $method,
            ($this->options['bucket_endpoint'] ?? false) || ! empty($this->options['temporary_url']) ? '' : $this->bucket,
            $this->applyPathPrefix($path),
            $expires
        );
        $preSignedURLInput->setQuery($options);
        if ($this->options['bucket_endpoint'] ?? false) {
            $preSignedURLInput->setAlternativeEndpoint($this->options['endpoint']);
        }

        if (! empty($this->options['temporary_url'])) {
            $preSignedURLInput->setAlternativeEndpoint($this->options['temporary_url']);
        }

        try {
            return $this->client->preSignedURL($preSignedURLInput)
                ->getSignedUrl();
        } catch (TosServerException $tosServerException) {
            return false;
        }
    }

    /**
     * Get a temporary URL for the file at the given path.
     *
     * @param \DateTimeInterface|int $expiration
     * @param array<string, mixed> $options
     *
     * @return string
     */
    public function getTemporaryUrl(string $path, $expiration, array $options = [], string $method = 'GET')
    {
        $signedUrl = $this->signUrl($path, $expiration, $options, $method);
        if ($signedUrl === false) {
            return false;
        }

        $uri = new Uri($signedUrl);

        if (isset($this->options['temporary_url'])) {
            $uri = $this->replaceBaseUrl($uri, $this->options['temporary_url']);
        }

        return (string) $uri;
    }

    /**
     * Concatenate a path to a URL.
     */
    protected function concatPathToUrl(string $url, string $path): string
    {
        return rtrim($url, '/') . '/' . ltrim($path, '/');
    }

    /**
     * Replace the scheme, host and port of the given UriInterface with values from the given URL.
     */
    protected function replaceBaseUrl(UriInterface $uri, string $url): UriInterface
    {
        /** @var array{scheme: string, host: string, port?: int} $parsed */
        $parsed = parse_url($url);

        return $uri
            ->withScheme($parsed['scheme'])
            ->withHost($parsed['host'])
            ->withPort($parsed['port'] ?? null);
    }

    public function update($path, $contents, Config $config): bool
    {
        return $this->upload($path, $contents, $config);
    }

    public function updateStream($path, $resource, Config $config): bool
    {
        return $this->upload($path, $resource, $config);
    }

    public function deleteDir($dirname): bool
    {
        $result = $this->listDirObjects($dirname, true);
        $keys = array_column($result['objects'], 'key');
        if ($keys === []) {
            return true;
        }

        try {
            foreach (array_chunk($keys, 1000) as $items) {
                $input = new DeleteMultiObjectsInput($this->bucket, array_map(
                    static function ($key): ObjectTobeDeleted {
                        return new ObjectTobeDeleted($key);
                    },
                    $items
                ));
                $this->client->deleteMultiObjects($input);
            }
        } catch (TosServerException $tosServerException) {
            return false;
        }

        return true;
    }

    public function createDir($dirname, Config $config): bool
    {
        return $this->upload(rtrim($dirname, '/') . '/', '', $config);
    }

    public function has($path): bool
    {
        $location = $this->applyPathPrefix($path);

        try {
            $headObjectInput = new HeadObjectInput($this->bucket, $location);
            if ($this->client->headObject($headObjectInput)) {
                return true;
            }

            $headObjectInput->setKey(rtrim($location, '/') . '/');

            return (bool) $this->client->headObject($headObjectInput);
        } catch (TosServerException $tosServerException) {
            return false;
        }
    }

    /**
     * @param mixed $path
     *
     * @return array|false
     */
    public function getSize($path)
    {
        return $this->getMetadata($path);
    }

    /**
     * @param mixed $path
     *
     * @return array|false
     */
    public function getMimetype($path)
    {
        return $this->getMetadata($path);
    }

    /**
     * @param mixed $path
     *
     * @return array|false
     */
    public function getTimestamp($path)
    {
        return $this->getMetadata($path);
    }

    /**
     * @param mixed $path
     *
     * @return false|string[]
     */
    public function getVisibility($path)
    {
        try {
            $getObjectACLInput = new GetObjectACLInput($this->bucket, $this->applyPathPrefix($path));
            $result = $this->client->getObjectAcl($getObjectACLInput);
        } catch (TosServerException $tosServerException) {
            return false;
        }

        return [
            'visibility' => $this->aclToVisibility($result),
        ];
    }

    public function visibilityToAcl(string $visibility): string
    {
        if ($visibility === AdapterInterface::VISIBILITY_PUBLIC) {
            return Enum::ACLPublicRead;
        }

        return Enum::ACLPrivate;
    }

    public function aclToVisibility(GetObjectACLOutput $model): string
    {
        foreach ($model->getGrants() as $grant) {
            $grantee = $grant->getGrantee();
            if ($grantee === null) {
                continue;
            }

            if (! \in_array($grantee->getCanned(), [Enum::CannedAuthenticatedUsers, Enum::CannedAllUsers], true)) {
                continue;
            }

            if ($grant->getPermission() !== Enum::PermissionRead) {
                continue;
            }

            return AdapterInterface::VISIBILITY_PUBLIC;
        }

        return AdapterInterface::VISIBILITY_PRIVATE;
    }
}
