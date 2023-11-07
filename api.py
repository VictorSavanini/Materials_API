from mp_api.client import MPRester

key = "qx3M9lC8cMaGU3gp0ZpKSPor69SxkCU9"

with MPRester(key) as mpr:
    dos = mpr.get_dos_by_material_id("mp-149")
    fp = dos.get_dos_fp()

    # print(dos.get_normalized())
    # print(dos.get_site_dos())
    # print(dos.get_densities())

    print(help(dos))
