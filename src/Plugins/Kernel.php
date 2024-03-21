<?php

declare(strict_types=1);

namespace Zing\Flysystem\Tos\Plugins;

use League\Flysystem\Plugin\AbstractPlugin;

class Kernel extends AbstractPlugin
{
    public function getMethod(): string
    {
        return 'kernel';
    }

    /**
     * @return mixed
     */
    public function handle()
    {
        return $this->filesystem->getAdapter()
            ->getClient();
    }
}
