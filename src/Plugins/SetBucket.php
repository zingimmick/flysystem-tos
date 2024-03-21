<?php

declare(strict_types=1);

namespace Zing\Flysystem\Tos\Plugins;

use League\Flysystem\Plugin\AbstractPlugin;

class SetBucket extends AbstractPlugin
{
    /**
     * sign url.
     */
    public function getMethod(): string
    {
        return 'bucket';
    }

    /**
     * handle.
     *
     * @param mixed $bucket
     *
     * @return mixed
     */
    public function handle($bucket)
    {
        return $this->filesystem->getAdapter()
            ->setBucket($bucket);
    }
}
