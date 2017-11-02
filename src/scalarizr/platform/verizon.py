from scalarizr.platform.openstack import OpenstackPlatform


def get_platform():
    return OpenstackPlatform()


class VerizonPlatform(OpenstackPlatform):
    name = 'verizon'
