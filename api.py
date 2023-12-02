from mp_api.client import MPRester

import plotly.graph_objects as go
import pandas as pd
import time
from numpy import mean
from datetime import date

import sqlite3
import os


def to_sql_list(itens: list) -> str:
    "Converte uma lista Python em uma lista formatada para consultas SQL."

    def _convert(x):
        if isinstance(x, str):
            x = x.replace("'", "''")
            return f"'{x}'"
        if (pd.isna(x)) or (x == "None"):
            return "NULL"
        elif isinstance(x, date):
            return f"'{x}'"
        else:
            return str(x)

    return f'({",".join(map(_convert, itens))})'


class MaterialsDataWriter:
    def __init__(self, api_key):
        print(
            """
Para reescrever os dados use o Método rewrite_database().
utilize o parametro max_time se precisar que o processo seja interrompido automaticamente.
"""
        )
        print("Coletando os ids de todos os materiais")
        self.ms = self._get_summary(api_key)

    def get_sites(self, mpe):
        dfs = []
        for coords in mpe.structure.sites:
            fracs = list(coords.__dict__["_frac_coords"])
            dfs.append(
                pd.DataFrame(
                    {
                        "element": [str(coords.specie)],
                        "x": [fracs[0]],
                        "y": [fracs[1]],
                        "z": [fracs[2]],
                    }
                )
            )
        return pd.concat(dfs, ignore_index=True).to_json(orient="index")

    def find_zero(self, df: pd.DataFrame) -> float:
        def interpol(dfi: pd.DataFrame):
            dfi.reset_index(drop=True, inplace=True)
            return -(
                dfi.energies[0] * dfi.densities[1] - dfi.energies[1] * dfi.densities[0]
            ) / (dfi.energies[1] - dfi.energies[0])

        serie = (
            df.reset_index(drop=True).energies.shift(1)
            * df.reset_index(drop=True).energies
        )
        turn_point = serie[serie < 0].index[0]

        return interpol(df[turn_point - 1 : turn_point + 1])

    # Encontrar DOS na Efermi para cada elemento
    def get_dos_at_efermi(self, material_id: str) -> tuple:
        "Encontra a densidade de estados na energia de Fermi para um material"
        dos = self.mpr.get_dos_by_material_id(material_id)
        ed = dos.get_element_dos()

        element_values = {}
        for element in ed:
            e_dos = ed[element]
            e_df = pd.DataFrame(
                {
                    "energies": e_dos.energies - e_dos.efermi,
                    "densities": e_dos.get_densities(),
                }
            )
            element_values[str(element)] = self.find_zero(e_df)

        dg = pd.DataFrame(
            {"energies": dos.energies - dos.efermi, "densities": dos.get_densities()}
        )

        return self.find_zero(dg), str(element_values)

    def _get_summary(self, api_key):
        """Puxa o sumário do materials projects apenas com os dados selecionados"""
        with MPRester(api_key) as mpr:
            self.mpr = mpr

            return mpr.summary.search()

    def rewrite_database(self, max_time: int = None):
        print(
            """
Essa função pode demorar dias para terminar, mas esse periodo pode ser dividido.
Essa função automaticamente cria uma tabela temporaria para que não sejam perdidas as infromações que são baixadas.
Você pode continuar usando os dados normalmente em outro arquivo.
Essa função pode ser interrompida a qualquer momento de perda de informações.
Depois que a tabela temporaria estiver completa, os dados irão para a tabela final.
        """
        )
        t1 = time.time()
        db_file = "mp_database.db"

        with sqlite3.connect(db_file) as conn:
            print("Iniciando o Upload")
            already_in_temp = [
                x[0] for x in conn.cursor().execute("select id from temp_materials")
            ]

            for mms in self.ms:
                mid = int(str(mms.material_id)[3:])

                if max_time:
                    if time.time() > (t1 + max_time):
                        raise Exception("Exceded time limit")

                if mid not in already_in_temp:
                    try:
                        all_dos = self.get_dos_at_efermi(str(mms.material_id))
                    except:
                        all_dos = [None, None]

                    txt = to_sql_list(
                        [
                            mid,
                            mms.nelements,
                            mms.nsites,
                            str(mms.composition),
                            mms.formula_pretty,
                            mms.volume,
                            mms.density,
                            mms.density_atomic,
                            str(mms.symmetry.crystal_system),
                            mms.symmetry.symbol,
                            mms.symmetry.number,
                            str(mms.material_id),
                            mms.is_stable,
                            mms.is_magnetic,
                            mms.is_metal,
                            mms.is_gap_direct,
                            mms.energy_per_atom,
                            mms.efermi,
                            mms.total_magnetization,
                            mms.last_updated,
                            mms.deprecated,
                            str(
                                {
                                    "abc": list(mms.structure.lattice.abc),
                                    "angles": list(mms.structure.lattice.angles),
                                }
                            ),
                            str(self.get_sites(mms)),
                            all_dos[0],
                            all_dos[1],
                        ]
                    )

                    conn.executescript(
                        f"""
                            insert into temp_materials (
                                id,
                                n_elements,
                                n_atoms,
                                composition,
                                formula,
                                volume,
                                density,
                                atomic_density,
                                symetry,
                                symetry_symbol,
                                symetry_number,
                                material_id,
                                is_stable,
                                is_magnetic,
                                is_metal,
                                is_gap_direct,
                                energy_per_atom,
                                efermi,
                                total_magnetization,
                                last_updated,
                                deprecated,
                                lattice_structure,
                                element_coords,
                                dos_at_efermi,
                                elements_dos_at_efermi
                            )
                            values
                            {txt}
                            """
                    )

            conn.executescript(
                """
                delete from materials;
                insert into materials
                select * from temp_materials;
                delete from temp_materials;
                """
            )
            print("Atualização Concluida com Sucesso!!!")
