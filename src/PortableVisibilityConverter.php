<?php

declare(strict_types=1);

namespace Zing\Flysystem\Tos;

use League\Flysystem\Visibility;
use Tos\Model\Enum;
use Tos\Model\GetObjectACLOutput;

class PortableVisibilityConverter implements VisibilityConverter
{
    /**
     * @var string
     */
    private const PUBLIC_ACL = Enum::ACLPublicRead;

    /**
     * @var string
     */
    private const PRIVATE_ACL = Enum::ACLPrivate;

    public function __construct(
        private string $default = Visibility::PUBLIC,
        private string $defaultForDirectories = Visibility::PUBLIC
    ) {
    }

    public function visibilityToAcl(string $visibility): string
    {
        if ($visibility === Visibility::PUBLIC) {
            return self::PUBLIC_ACL;
        }

        return self::PRIVATE_ACL;
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

            return Visibility::PUBLIC;
        }

        return Visibility::PRIVATE;
    }

    public function defaultForDirectories(): string
    {
        return $this->defaultForDirectories;
    }

    public function getDefault(): string
    {
        return $this->default;
    }
}
