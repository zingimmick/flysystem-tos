<?php

declare(strict_types=1);

namespace Zing\Flysystem\Tos;

use Tos\Model\GetObjectACLOutput;

interface VisibilityConverter
{
    public function visibilityToAcl(string $visibility): string;

    public function aclToVisibility(GetObjectACLOutput $model): string;

    public function defaultForDirectories(): string;
}
