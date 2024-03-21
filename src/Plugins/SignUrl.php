<?php

declare(strict_types=1);

namespace Zing\Flysystem\Tos\Plugins;

use League\Flysystem\Plugin\AbstractPlugin;

class SignUrl extends AbstractPlugin
{
    /**
     * sign url.
     */
    public function getMethod(): string
    {
        return 'signUrl';
    }

    /**
     * handle.
     *
     * @param mixed $path
     * @param \DateTimeInterface|int $expiration
     * @param mixed $method
     *
     * @return mixed
     */
    public function handle($path, $expiration, array $options = [], $method = 'GET')
    {
        return $this->filesystem->getAdapter()
            ->signUrl($path, $expiration, $options, $method);
    }
}
