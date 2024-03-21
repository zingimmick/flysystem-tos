<?php

declare(strict_types=1);

namespace Zing\Flysystem\Tos\Plugins;

use League\Flysystem\Plugin\AbstractPlugin;

class TemporaryUrl extends AbstractPlugin
{
    /**
     * getTemporaryUrl.
     */
    public function getMethod(): string
    {
        return 'getTemporaryUrl';
    }

    /**
     * handle.
     *
     * @param \DateTimeInterface|int $expiration
     * @param mixed $method
     *
     * @return mixed
     */
    public function handle(string $path, $expiration, array $options = [], $method = 'GET')
    {
        return $this->filesystem->getAdapter()
            ->getTemporaryUrl($path, $expiration, $options, $method);
    }
}
