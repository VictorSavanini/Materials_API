

from mp_api.client import MPRester

key='qx3M9lC8cMaGU3gp0ZpKSPor69SxkCU9'

api = MPRester(key)

dos = api.get_bandstructure_by_material_id("mp-20470")


print(dos)