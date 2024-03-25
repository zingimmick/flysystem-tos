<?php

declare(strict_types=1);

namespace Zing\Flysystem\Tos;

use GuzzleHttp\Psr7\Uri;
use League\Flysystem\ChecksumAlgoIsNotSupported;
use League\Flysystem\ChecksumProvider;
use League\Flysystem\Config;
use League\Flysystem\DirectoryAttributes;
use League\Flysystem\FileAttributes;
use League\Flysystem\FilesystemAdapter;
use League\Flysystem\FilesystemOperationFailed;
use League\Flysystem\PathPrefixer;
use League\Flysystem\StorageAttributes;
use League\Flysystem\UnableToCheckDirectoryExistence;
use League\Flysystem\UnableToCopyFile;
use League\Flysystem\UnableToCreateDirectory;
use League\Flysystem\UnableToDeleteDirectory;
use League\Flysystem\UnableToDeleteFile;
use League\Flysystem\UnableToGeneratePublicUrl;
use League\Flysystem\UnableToGenerateTemporaryUrl;
use League\Flysystem\UnableToListContents;
use League\Flysystem\UnableToMoveFile;
use League\Flysystem\UnableToProvideChecksum;
use League\Flysystem\UnableToReadFile;
use League\Flysystem\UnableToRetrieveMetadata;
use League\Flysystem\UnableToSetVisibility;
use League\Flysystem\UnableToWriteFile;
use League\Flysystem\UrlGeneration\PublicUrlGenerator;
use League\Flysystem\UrlGeneration\TemporaryUrlGenerator;
use League\Flysystem\Visibility;
use League\MimeTypeDetection\FinfoMimeTypeDetector;
use League\MimeTypeDetection\MimeTypeDetector;
use Psr\Http\Message\UriInterface;
use Tos\Exception\TosServerException;
use Tos\Model\Constant;
use Tos\Model\CopyObjectInput;
use Tos\Model\DeleteMultiObjectsInput;
use Tos\Model\DeleteObjectInput;
use Tos\Model\GetObjectACLInput;
use Tos\Model\GetObjectInput;
use Tos\Model\HeadObjectInput;
use Tos\Model\ListObjectsInput;
use Tos\Model\ObjectTobeDeleted;
use Tos\Model\PreSignedURLInput;
use Tos\Model\PutObjectACLInput;
use Tos\Model\PutObjectInput;
use Tos\TosClient;

class TosAdapter implements FilesystemAdapter, PublicUrlGenerator, ChecksumProvider, TemporaryUrlGenerator
{
    /**
     * @var string[]
     */
    private const EXTRA_METADATA_FIELDS = [Constant::HeaderStorageClass, Constant::HeaderETag];

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

    private PathPrefixer $pathPrefixer;

    private PortableVisibilityConverter|VisibilityConverter $visibilityConverter;

    private FinfoMimeTypeDetector|MimeTypeDetector $mimeTypeDetector;

    /**
     * @param array{url?: string, temporary_url?: string, endpoint?: string, bucket_endpoint?: bool} $options
     */
    public function __construct(
        protected TosClient $tosClient,
        protected string $bucket,
        string $prefix = '',
        ?VisibilityConverter $visibility = null,
        ?MimeTypeDetector $mimeTypeDetector = null,
        /**
         * @phpstan-var array{url?: string, temporary_url?: string, endpoint?: string, bucket_endpoint?: bool}
         */
        protected array $options = []
    ) {
        $this->pathPrefixer = new PathPrefixer($prefix);
        $this->visibilityConverter = $visibility instanceof VisibilityConverter ? $visibility : new PortableVisibilityConverter();
        $this->mimeTypeDetector = $mimeTypeDetector instanceof MimeTypeDetector ? $mimeTypeDetector : new FinfoMimeTypeDetector();
    }

    public function getBucket(): string
    {
        return $this->bucket;
    }

    public function getClient(): TosClient
    {
        return $this->tosClient;
    }

    public function write(string $path, string $contents, Config $config): void
    {
        $this->upload($path, $contents, $config);
    }

    public function kernel(): TosClient
    {
        return $this->getClient();
    }

    public function setBucket(string $bucket): void
    {
        $this->bucket = $bucket;
    }

    /**
     * @param resource $contents
     */
    public function writeStream(string $path, $contents, Config $config): void
    {
        $this->upload($path, $contents, $config);
    }

    /**
     * @param string|resource $contents
     */
    private function upload(string $path, $contents, Config $config): void
    {
        $options = $this->createOptionsFromConfig($config);
        $putObjectInput = new PutObjectInput($this->bucket, $this->pathPrefixer->prefixPath($path), $contents);
        if (! isset($options[Constant::HeaderAcl])) {
            /** @var string|null $visibility */
            $visibility = $config->get(Config::OPTION_VISIBILITY);
            if ($visibility !== null) {
                $options[Constant::HeaderAcl] ??= $this->visibilityConverter->visibilityToAcl($visibility);
            }
        }

        $shouldDetermineMimetype = $contents !== '' && ! \array_key_exists(Constant::HeaderContentType, $options);

        if ($shouldDetermineMimetype) {
            $mimeType = $this->mimeTypeDetector->detectMimeType($path, $contents);
            if ($mimeType !== null && $mimeType !== '') {
                $options[Constant::HeaderContentType] = $mimeType;
            }
        }

        if (isset($options[Constant::HeaderAcl])) {
            $putObjectInput->setACL($options[Constant::HeaderAcl]);
        }

        if (isset($options[Constant::HeaderContentType])) {
            $putObjectInput->setContentType($options[Constant::HeaderContentType]);
        }

        if (isset($options[Constant::HeaderExpires])) {
            $putObjectInput->setExpires($options[Constant::HeaderExpires]);
        }

        try {
            $this->tosClient->putObject($putObjectInput);
        } catch (TosServerException $tosServerException) {
            throw UnableToWriteFile::atLocation($path, $tosServerException->getMessage(), $tosServerException);
        }
    }

    public function move(string $source, string $destination, Config $config): void
    {
        try {
            $this->copy($source, $destination, $config);
            $this->delete($source);
        } catch (FilesystemOperationFailed $filesystemOperationFailed) {
            throw UnableToMoveFile::fromLocationTo($source, $destination, $filesystemOperationFailed);
        }
    }

    public function copy(string $source, string $destination, Config $config): void
    {
        try {
            /** @var string|null $visibility */
            $visibility = $config->get(Config::OPTION_VISIBILITY);
            if ($visibility === null && $config->get('retain_visibility', true)) {
                $visibility = $this->visibility($source)
                    ->visibility();
            }
        } catch (FilesystemOperationFailed $filesystemOperationFailed) {
            throw UnableToCopyFile::fromLocationTo($source, $destination, $filesystemOperationFailed);
        }

        $config = $config->withDefaults([
            Config::OPTION_VISIBILITY => $visibility ?: Visibility::PRIVATE,
        ]);

        try {
            $copyObjectInput = new CopyObjectInput(
                $this->bucket,
                $this->pathPrefixer->prefixPath($destination),
                $this->bucket,
                $this->pathPrefixer->prefixPath($source)
            );
            $copyObjectInput->setACL(
                $this->visibilityConverter->visibilityToAcl($config->get(Config::OPTION_VISIBILITY))
            );
            $this->tosClient->copyObject($copyObjectInput);
        } catch (TosServerException $tosServerException) {
            throw UnableToCopyFile::fromLocationTo($source, $destination, $tosServerException);
        }
    }

    public function delete(string $path): void
    {
        try {
            $deleteObjectInput = new DeleteObjectInput($this->bucket, $this->pathPrefixer->prefixPath($path));
            $this->tosClient->deleteObject($deleteObjectInput);
        } catch (TosServerException $tosServerException) {
            throw UnableToDeleteFile::atLocation($path, $tosServerException->getMessage(), $tosServerException);
        }
    }

    public function deleteDirectory(string $path): void
    {
        $result = $this->listDirObjects($path, true);
        $keys = array_column($result['objects'], 'key');
        if ($keys === []) {
            return;
        }

        try {
            foreach (array_chunk($keys, 1000) as $items) {
                $input = new DeleteMultiObjectsInput($this->bucket, array_map(
                    static fn ($key): ObjectTobeDeleted => new ObjectTobeDeleted($key),
                    $items
                ));
                $this->tosClient->deleteMultiObjects($input);
            }
        } catch (TosServerException $tosServerException) {
            throw UnableToDeleteDirectory::atLocation($path, $tosServerException->getMessage(), $tosServerException);
        }
    }

    public function createDirectory(string $path, Config $config): void
    {
        $defaultVisibility = $config->get('directory_visibility', $this->visibilityConverter->defaultForDirectories());
        $config = $config->withDefaults([
            'visibility' => $defaultVisibility,
        ]);

        try {
            $this->write(trim($path, '/') . '/', '', $config);
        } catch (FilesystemOperationFailed $filesystemOperationFailed) {
            throw UnableToCreateDirectory::dueToFailure($path, $filesystemOperationFailed);
        }
    }

    public function setVisibility(string $path, string $visibility): void
    {
        try {
            $putObjectACLInput = new PutObjectACLInput(
                $this->bucket,
                $this->pathPrefixer->prefixPath($path),
                $this->visibilityConverter->visibilityToAcl($visibility)
            );
            $this->tosClient->putObjectACL($putObjectACLInput);
        } catch (TosServerException $tosServerException) {
            throw UnableToSetVisibility::atLocation($path, $tosServerException->getMessage(), $tosServerException);
        }
    }

    public function visibility(string $path): FileAttributes
    {
        try {
            $getObjectACLInput = new GetObjectACLInput($this->bucket, $this->pathPrefixer->prefixPath($path));
            $result = $this->tosClient->getObjectACL($getObjectACLInput);
            if ($result === null) {
                throw UnableToRetrieveMetadata::visibility($path, 'The TOS server returns NULL.');
            }
        } catch (TosServerException $tosServerException) {
            throw UnableToRetrieveMetadata::visibility($path, $tosServerException->getMessage(), $tosServerException);
        }

        $visibility = $this->visibilityConverter->aclToVisibility($result);

        return new FileAttributes($path, null, $visibility);
    }

    public function fileExists(string $path): bool
    {
        try {
            $headObjectInput = new HeadObjectInput($this->bucket, $this->pathPrefixer->prefixPath($path));

            return (bool) $this->tosClient->headObject($headObjectInput);
        } catch (TosServerException) {
            return false;
        }
    }

    public function directoryExists(string $path): bool
    {
        try {
            $prefix = $this->pathPrefixer->prefixDirectoryPath($path);
            $listObjectsInput = new ListObjectsInput($this->bucket, 1, $prefix);
            $listObjectsInput->setDelimiter('/');
            $model = $this->tosClient->listObjects($listObjectsInput);
            if ($model === null) {
                throw new UnableToCheckDirectoryExistence(
                    sprintf('Unable to check existence for: %s. The TOS server returns NULL.', $path)
                );
            }

            return $model->getContents() !== [];
        } catch (TosServerException $tosServerException) {
            throw UnableToCheckDirectoryExistence::forLocation($path, $tosServerException);
        }
    }

    public function read(string $path): string
    {
        try {
            $getObjectInput = new GetObjectInput($this->bucket, $this->pathPrefixer->prefixPath($path));
            $result = $this->tosClient->getObject($getObjectInput)
                ->getContent();
            if ($result === null) {
                throw UnableToReadFile::fromLocation($path, 'The TOS server returns NULL.');
            }

            return $result->getContents();
        } catch (TosServerException $tosServerException) {
            throw UnableToReadFile::fromLocation($path, $tosServerException->getMessage(), $tosServerException);
        }
    }

    /**
     * @return resource
     */
    public function readStream(string $path)
    {
        try {
            $getObjectInput = new GetObjectInput($this->bucket, $this->pathPrefixer->prefixPath($path));
            $getObjectInput->setStreamMode(false);
            $result = $this->tosClient->getObject($getObjectInput)
                ->getContent();
            if ($result === null) {
                throw UnableToReadFile::fromLocation($path, 'The TOS server returns NULL.');
            }

            $stream = $result->detach();
            if ($stream === null) {
                throw UnableToReadFile::fromLocation($path, 'The TOS server returns NULL.');
            }
        } catch (TosServerException $tosServerException) {
            throw UnableToReadFile::fromLocation($path, $tosServerException->getMessage(), $tosServerException);
        }

        return $stream;
    }

    /**
     * @return \Traversable<\League\Flysystem\StorageAttributes>
     */
    public function listContents(string $path, bool $deep): iterable
    {
        $directory = rtrim($path, '/');
        $result = $this->listDirObjects($directory, $deep);

        foreach ($result['objects'] as $files) {
            $path = $this->pathPrefixer->stripDirectoryPrefix((string) ($files['key'] ?? $files['prefix']));
            if ($path === $directory) {
                continue;
            }

            yield $this->mapObjectMetadata($files);
        }

        foreach ($result['prefix'] as $dir) {
            yield new DirectoryAttributes($this->pathPrefixer->stripDirectoryPrefix($dir));
        }
    }

    /**
     * Get the metadata of a file.
     */
    private function getMetadata(string $path, string $type): FileAttributes
    {
        try {
            $headObjectInput = new HeadObjectInput($this->bucket, $this->pathPrefixer->prefixPath($path));
            $metadata = $this->tosClient->headObject($headObjectInput);
        } catch (TosServerException $tosServerException) {
            throw UnableToRetrieveMetadata::create(
                $path,
                $type,
                $tosServerException->getMessage(),
                $tosServerException
            );
        }

        $attributes = $this->mapObjectMetadata([
            'key' => $headObjectInput->getKey(),
            'last-modified' => $metadata->getLastModified(),
            'content-type' => $metadata->getContentType(),
            'size' => $metadata->getContentLength(),
            Constant::HeaderETag => $metadata->getETag(),
            Constant::HeaderStorageClass => $metadata->getStorageClass(),
        ], $path);

        if (! $attributes instanceof FileAttributes) {
            throw UnableToRetrieveMetadata::create($path, $type);
        }

        return $attributes;
    }

    /**
     * @param array{key?: string, prefix?: string|null, content-length?: int, size?: int, last-modified?: int, content-type?: string} $metadata
     */
    private function mapObjectMetadata(array $metadata, ?string $path = null): StorageAttributes
    {
        if ($path === null) {
            $path = $this->pathPrefixer->stripPrefix((string) ($metadata['key'] ?? $metadata['prefix']));
        }

        if (str_ends_with($path, '/')) {
            return new DirectoryAttributes(rtrim($path, '/'));
        }

        return new FileAttributes(
            $path,
            $metadata['content-length'] ?? $metadata['size'] ?? null,
            null,
            $metadata['last-modified'] ?? null,
            $metadata['content-type'] ?? null,
            $this->extractExtraMetadata($metadata)
        );
    }

    /**
     * @param array<string,mixed> $metadata
     *
     * @return array<string,mixed>
     */
    private function extractExtraMetadata(array $metadata): array
    {
        $extracted = [];

        foreach (self::EXTRA_METADATA_FIELDS as $field) {
            if (! isset($metadata[$field])) {
                continue;
            }

            if ($metadata[$field] === '') {
                continue;
            }

            $extracted[$field] = $metadata[$field];
        }

        return $extracted;
    }

    public function fileSize(string $path): FileAttributes
    {
        $attributes = $this->getMetadata($path, FileAttributes::ATTRIBUTE_FILE_SIZE);
        if ($attributes->fileSize() === null) {
            throw UnableToRetrieveMetadata::fileSize($path);
        }

        return $attributes;
    }

    public function mimeType(string $path): FileAttributes
    {
        $attributes = $this->getMetadata($path, FileAttributes::ATTRIBUTE_MIME_TYPE);
        if ($attributes->mimeType() === null) {
            throw UnableToRetrieveMetadata::mimeType($path);
        }

        return $attributes;
    }

    public function lastModified(string $path): FileAttributes
    {
        $attributes = $this->getMetadata($path, FileAttributes::ATTRIBUTE_LAST_MODIFIED);
        if ($attributes->lastModified() === null) {
            throw UnableToRetrieveMetadata::lastModified($path);
        }

        return $attributes;
    }

    /**
     * File list core method.
     *
     * @return array{prefix: array<string>, objects: array<array{key?: string, prefix: ?string, content-length?: int, size?: int, last-modified?: int, content-type?: string}>}
     */
    public function listDirObjects(string $dirname = '', bool $recursive = false): array
    {
        $prefix = trim($this->pathPrefixer->prefixPath($dirname), '/');
        $prefix = $prefix === '' ? '' : $prefix . '/';

        $nextMarker = '';

        $result = [];

        while (true) {
            $input = new ListObjectsInput($this->bucket, self::MAX_KEYS, $prefix, $nextMarker);
            $input->setDelimiter($recursive ? '' : self::DELIMITER);
            $model = $this->tosClient->listObjects($input);
            if ($model === null) {
                throw new UnableToListContents(sprintf("Unable to list contents for '%s', ", $prefix)
                    . ($recursive ? 'deep' : 'shallow') . " listing\n\n"
                    . 'Reason: The TOS server returns NULL.');
            }

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
     * @param array{prefix?: array<string>, objects?: array<array{prefix: string, key: string, last-modified: int, size: int, ETag: string, x-tos-storage-class: string}>} $result
     * @param array<\Tos\Model\ListedObject> $objects
     *
     * @return array{prefix?: array<string>, objects: array<array{prefix: string, key: string, last-modified: int, size: int, ETag: string, x-tos-storage-class: string}>}
     */
    private function processObjects(array $result, array $objects, string $dirname): array
    {
        $result['objects'] = array_map(static fn ($object): array => [
            'prefix' => $dirname,
            'key' => $object->getKey(),
            'last-modified' => $object->getLastModified(),
            'size' => $object->getSize(),
            Constant::HeaderETag => $object->getETag(),
            Constant::HeaderStorageClass => $object->getStorageClass(),
        ], $objects);

        return $result;
    }

    /**
     * @param array{prefix?: array<string>, objects: array<array{prefix: string, key: string, last-modified: int, size: int, ETag: string, x-tos-storage-class: string}>} $result
     * @param array<\Tos\Model\ListedCommonPrefix> $prefixes
     *
     * @return array{prefix: array<string>, objects: array<array{prefix: string, key: string, last-modified: int, size: int, ETag: string, x-tos-storage-class: string}>}
     */
    private function processPrefixes(array $result, array $prefixes): array
    {
        $result['prefix'] = array_map(static fn ($prefix) => $prefix->getPrefix(), $prefixes);

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

        /** @var string|null $visibility */
        $visibility = $config->get(Config::OPTION_VISIBILITY);
        if ($visibility) {
            $options[Constant::HeaderAcl] = $this->visibilityConverter->visibilityToAcl($visibility);
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
            return $this->concatPathToUrl($this->options['url'], $this->pathPrefixer->prefixPath($path));
        }

        return $this->concatPathToUrl($this->normalizeHost(), $this->pathPrefixer->prefixPath($path));
    }

    protected function normalizeHost(): string
    {
        if (! isset($this->options['endpoint'])) {
            throw UnableToGetUrl::missingOption('endpoint');
        }

        $endpoint = $this->options['endpoint'];
        if (! str_starts_with($endpoint, 'http')) {
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
     * @param array<string, mixed> $options
     */
    public function signUrl(
        string $path,
        \DateTimeInterface|int $expiration,
        array $options = [],
        string $method = 'GET'
    ): string {
        $expires = $expiration instanceof \DateTimeInterface ? $expiration->getTimestamp() - time() : $expiration;
        $preSignedURLInput = new PreSignedURLInput(
            $method,
            empty($this->options['temporary_url']) ? $this->bucket : '',
            $this->pathPrefixer->prefixPath($path),
            $expires
        );
        if (! empty($this->options['temporary_url'])) {
            $preSignedURLInput->setAlternativeEndpoint($this->options['temporary_url']);
        }
        $preSignedURLInput->setQuery($options);

        return $this->tosClient->preSignedURL($preSignedURLInput)->getSignedUrl();
    }

    /**
     * Get a temporary URL for the file at the given path.
     *
     * @param array<string, mixed> $options
     */
    public function getTemporaryUrl(
        string $path,
        \DateTimeInterface|int $expiration,
        array $options = [],
        string $method = 'GET'
    ): string {
        $uri = new Uri($this->signUrl($path, $expiration, $options, $method));

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

    public function publicUrl(string $path, Config $config): string
    {
        $location = $this->pathPrefixer->prefixPath($path);

        try {
            return $this->concatPathToUrl($this->normalizeHost(), $location);
        } catch (\Throwable $throwable) {
            throw UnableToGeneratePublicUrl::dueToError($path, $throwable);
        }
    }

    public function checksum(string $path, Config $config): string
    {
        $algo = $config->get('checksum_algo', Constant::HeaderETag);

        if ($algo !== Constant::HeaderETag) {
            throw new ChecksumAlgoIsNotSupported();
        }

        try {
            $metadata = $this->getMetadata($path, 'checksum')
                ->extraMetadata();
        } catch (UnableToRetrieveMetadata $unableToRetrieveMetadata) {
            throw new UnableToProvideChecksum($unableToRetrieveMetadata->reason(), $path, $unableToRetrieveMetadata);
        }

        if (! isset($metadata[Constant::HeaderETag])) {
            throw new UnableToProvideChecksum('etag header not available.', $path);
        }

        return strtolower(trim($metadata[Constant::HeaderETag], '"'));
    }

    public function temporaryUrl(string $path, \DateTimeInterface $expiresAt, Config $config): string
    {
        try {
            $preSignedURLInput = new PreSignedURLInput(
                'GET',
                $this->bucket,
                $this->pathPrefixer->prefixPath($path),
                $expiresAt->getTimestamp() - time(),
            );
            $preSignedURLInput->setQuery((array) $config->get('gcp_signing_options', []));

            return $this->tosClient->preSignedURL($preSignedURLInput)
                ->getSignedUrl();
        } catch (\Throwable $throwable) {
            throw UnableToGenerateTemporaryUrl::dueToError($path, $throwable);
        }
    }
}
