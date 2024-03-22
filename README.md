# Flysystem TOS

<p align="center">
<a href="https://github.com/zingimmick/flysystem-tos/actions/workflows/tests.yml"><img src="https://github.com/zingimmick/flysystem-tos/actions/workflows/tests.yml/badge.svg?branch=3.x" alt="tests"></a>
<a href="https://codecov.io/gh/zingimmick/flysystem-tos"><img src="https://codecov.io/gh/zingimmick/flysystem-tos/branch/3.x/graph/badge.svg" alt="Code Coverage" /></a>
<a href="https://packagist.org/packages/zing/flysystem-tos"><img src="https://poser.pugx.org/zing/flysystem-tos/v/stable.svg" alt="Latest Stable Version"></a>
<a href="https://packagist.org/packages/zing/flysystem-tos"><img src="https://poser.pugx.org/zing/flysystem-tos/downloads" alt="Total Downloads"></a>
<a href="https://packagist.org/packages/zing/flysystem-tos"><img src="https://poser.pugx.org/zing/flysystem-tos/v/unstable.svg" alt="Latest Unstable Version"></a>
<a href="https://packagist.org/packages/zing/flysystem-tos"><img src="https://poser.pugx.org/zing/flysystem-tos/license" alt="License"></a>
</p>

> **Requires**
> - **[PHP 8.0+](https://php.net/releases/)**
> - **[Flysystem 3.10+](https://github.com/thephpleague/flysystem/releases)**

## Version Information

| Version | Flysystem | PHP Version | Status                  |
|:--------|:----------|:------------|:------------------------|
| 3.x     | 3.10+     | >= 8.0      | Active support :rocket: |
| 2.x     | 2.x - 3.x | >= 7.2      | Active support          |
| 1.x     | 1.x       | >= 7.2      | Active support          |

Require Flysystem TOS using [Composer](https://getcomposer.org):

```bash
composer require zing/flysystem-tos
```

## Usage

```php
use League\Flysystem\Filesystem;
use Tos\TosClient;
use Zing\Flysystem\Tos\TosAdapter;

$prefix = '';
$config = [
    'key' => 'aW52YWxpZC1rZXk=',
    'secret' => 'aW52YWxpZC1zZWNyZXQ=',
    'bucket' => 'test',
    'endpoint' => 'tos-cn-shanghai.volces.com',
];

$config['options'] = [
    'url' => '',
    'endpoint' => $config['endpoint'], 
    'bucket_endpoint' => '',
    'temporary_url' => '',
];

$client = new TosClient($config['key'], $config['secret'], $config['endpoint']);
$adapter = new TosAdapter($client, $config['bucket'], $prefix, null, null, $config['options']);
$flysystem = new Filesystem($adapter);
```

## Integration

- Laravel: [zing/laravel-flysystem-tos](https://github.com/zingimmick/laravel-flysystem-tos)

## Reference

[league/flysystem-aws-s3-v3](https://github.com/thephpleague/flysystem-aws-s3-v3)

## License

Flysystem TOS is an open-sourced software licensed under the [MIT license](LICENSE).
