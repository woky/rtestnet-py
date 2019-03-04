import os
import json

from pathlib import Path

from libcloud.compute.drivers.gce import GCENodeDriver
from libcloud.dns.drivers.google import GoogleDNSDriver
from libcloud.common.google import ResourceNotFoundError
from libcloud.dns.types import RecordDoesNotExistError

from . import NodeError


# TODO make it "abc"
class NodeOps:
    pass


class NodeOpsError(NodeError):
    pass


class NodeOpsGCE(NodeOps):
    def __init__(self, config, credentials_file=None):
        self.conf = config
        if not credentials_file:
            try:
                credentials_file = os.environ['GOOGLE_APPLICATION_CREDENTIALS']
            except KeyError:
                raise NodeOpsError(
                    'You must pass path to credentials file either as ' +
                    'credentials_file argument or in ' +
                    'GOOGLE_APPLICATION_CREDENTIALS environment variable'
                ) from None
        try:
            credentials = json.loads(Path(credentials_file).read_text())
        except Exception as e:
            raise NodeOpsError(
                f'Could not read credentials_file = "{credentials_file}"'
            ) from e
        self._compute = GCENodeDriver(
            credentials['client_email'],
            credentials_file,
            project=credentials['project_id'],
            datacenter=config['gce_zone']
        )
        self._dns = GoogleDNSDriver(
            credentials['client_email'],
            credentials_file,
            project=credentials['project_id']
        )

    @property
    def _vm_name(self):
        return self.conf['gce_name']

    @property
    def _data_disk_name(self):
        return self._vm_name + '-data'

    @property
    def _fqdn(self):
        return self._vm_name + '.' + self.conf['gdns_domain']

    def _create_dns_rec(self, str_addr, fqdn, ttl):
        dns_zone = self._dns.get_zone(self.conf['gdns_zone'])
        data = {'ttl': ttl, 'rrdatas': [str_addr]}
        return self._dns.create_record(fqdn, dns_zone, 'A', data)

    def _maybe_get_dns_rec(self, fqdn):
        try:
            return self._dns.get_record(self.conf['gdns_zone'], 'A:' + fqdn)
        except RecordDoesNotExistError:
            return None

    def _get_dns_rec(self, str_addr, fqdn=None, ttl=300):
        if not fqdn:
            fqdn = self._fqdn
        rec = self._maybe_get_dns_rec(fqdn)
        if rec and str_addr not in rec.data['rrdatas']:
            self._dns.delete_record(rec)
            rec = None
        if not rec:
            rec = self._create_dns_rec(str_addr, fqdn, ttl)
        return rec

    def _delete_dns_rec(self, fqdn=None):
        if not fqdn:
            fqdn = self._fqdn
        rec = self._maybe_get_dns_rec(fqdn)
        if rec:
            self._dns.delete_record(rec)

    def _create_addr(self):
        addr = self._compute.ex_create_address(self._vm_name)
        return addr

    def _maybe_get_addr(self):
        try:
            return self._compute.ex_get_address(self._vm_name)
        except ResourceNotFoundError:
            return None

    def _get_addr(self):
        addr = self._maybe_get_addr()
        if not addr:
            addr = self._create_addr()
        return addr

    def _delete_addr(self):
        self._delete_dns_rec(self._fqdn)
        addr = self._maybe_get_addr()
        if addr:
            self._compute.ex_destroy_address(self._vm_name)

    def _create_data_disk(self):
        disk_type = 'pd-standard'
        if self.conf.get('data_disk_type_ssd', False):
            disk_type = 'pd-ssd'
        return self._compute.create_volume(
            self.conf['data_disk_size'],
            self._data_disk_name,
            ex_disk_type=disk_type
        )

    def _maybe_get_data_disk(self):
        try:
            return self._compute.ex_get_volume(self._data_disk_name)
        except ResourceNotFoundError:
            return None

    def _get_data_disk(self):
        disk = self._maybe_get_data_disk()
        if not disk:
            disk = self._create_data_disk()
        return disk

    def _create_vm(self):
        addr = self._get_addr()
        vm = self._compute.create_node(
            self._vm_name,
            size=self.conf['gce_machine_type'],
            image=self.conf['gce_boot_image'],
            external_ip=addr,
            ex_network=self.conf['gce_vpc_net'],
            ex_subnetwork=self.conf['gce_vpc_subnet'],
            ex_tags=self.conf['gce_tags']
        )
        disk = self._get_data_disk()
        self._compute.attach_volume(vm, disk, ex_auto_delete=True)
        self._get_dns_rec(addr.address)
        return vm

    def _maybe_get_vm(self):
        try:
            return self._compute.ex_get_node(self._vm_name)
        except ResourceNotFoundError:
            return None

    def _get_vm(self):
        vm = self._maybe_get_vm()
        if not vm:
            vm = self._create_vm()
        return vm

    def _delete_vm(self):
        vm = self._maybe_get_vm()
        if vm:
            vm.destroy()

    @property
    def config(self):
        return self.conf

    def stop(self, clean=False, mrproper=False):
        if clean or mrproper:
            self._delete_vm()
        else:
            vm = self._maybe_get_vm()
            if vm:
                self._compute.ex_stop_node(vm)
        if mrproper:
            self._delete_dns_rec()
            self._delete_addr()

    def start(self):
        self._get_vm()

    def add_dns_rec(self, name, ttl):
        addr = self._maybe_get_addr()
        if not addr:
            # TODO warn
            return
        fqdn = name + '.' + self.conf['gdns_domain']
        self._get_dns_rec(addr.address, fqdn=fqdn, ttl=ttl)
