# Flysystem TOS
<p align="center">
<a href="https://github.com/zingimmick/flysystem-tos/actions"><img src="https://github.com/zingimmick/flysystem-tos/workflows/tests/badge.svg" alt="Build Status"></a>
<a href="https://codecov.io/gh/zingimmick/flysystem-tos"><img src="https://codecov.io/gh/zingimmick/flysystem-tos/branch/1.x/graph/badge.svg" alt="Code Coverage" /></a>
<a href="https://packagist.org/packages/zing/flysystem-tos"><img src="https://poser.pugx.org/zing/flysystem-tos/v/stable.svg" alt="Latest Stable Version"></a>
<a href="https://packagist.org/packages/zing/flysystem-tos"><img src="https://poser.pugx.org/zing/flysystem-tos/downloads" alt="Total Downloads"></a>
<a href="https://packagist.org/packages/zing/flysystem-tos"><img src="https://poser.pugx.org/zing/flysystem-tos/v/unstable.svg" alt="Latest Unstable Version"></a>
<a href="https://packagist.org/packages/zing/flysystem-tos"><img src="https://poser.pugx.org/zing/flysystem-tos/license" alt="License"></a>
</p>

> **Requires [PHP 7.2.0+](https://php.net/releases/)**

Require Flysystem TOS using [Composer](https://getcomposer.org):

```bash
composer require zing/flysystem-tos:^1.0
```

## Usage

```php
use League\Flysystem\AdapterInterface;
use League\Flysystem\Filesystem;
use Tos\TosClient;
use Zing\Flysystem\Tos\TosAdapter;

$prefix = '';
$config = [
    'ak' => 'aW52YWxpZC1rZXk=',
    'sk' => 'aW52YWxpZC1zZWNyZXQ=',
    'region' => 'cn-shanghai',
    'bucket' => 'test',
    'endpoint' => 'tos-cn-shanghai.volces.com',
];

$config['options'] = [
    'url' => '',
    'endpoint' => $config['endpoint'], 
    'bucket_endpoint' => '',
    'temporary_url' => '',
    'default_visibility' => AdapterInterface::VISIBILITY_PUBLIC
];

$client = new TosClient($config);
$adapter = new TosAdapter($client, $config['bucket'], $prefix, $config['options']);
$flysystem = new Filesystem($adapter);
```

## Reference

[league/flysystem-aws-s3-v3](https://github.com/thephpleague/flysystem-aws-s3-v3)

[zing/flysystem-obs](https://github.com/zingimmick/flysystem-obs)

## License

Flysystem TOS is an open-sourced software licensed under the [MIT license](LICENSE).
