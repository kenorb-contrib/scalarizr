from scalarizr.platform.cloudstack import CloudStackPlatform


def get_platform():
    return UCloudPlatform()


class UCloudPlatform(CloudStackPlatform):
    name = 'ucloud'
