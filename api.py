from mp_api.client import MPRester

import pandas as pd
import time
from numpy import mean

import sqlite3
from datetime import datetime, date
from typing import List, Optional


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
        "Encontra o valor zero de energias e interpola para um unico DOS"

        def interpol(dfi: pd.DataFrame):
            "Interpola um DataFrame 2x2"
            dfi.reset_index(drop=True, inplace=True)
            return (
                dfi.energies[1] * dfi.densities[0] - dfi.energies[0] * dfi.densities[1]
            ) / (dfi.energies[1] - dfi.energies[0])

        serie = (
            df.reset_index(drop=True).energies.shift(1)
            * df.reset_index(drop=True).energies
        )
        turn_point = serie[serie < 0].index[0]

        return interpol(df[turn_point - 1 : turn_point + 1])

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

        dos_df = pd.DataFrame(
            {"energies": dos.energies - dos.efermi, "densities": dos.get_densities()}
        )

        return self.find_zero(dos_df), str(element_values)

    def _get_summary(self, api_key):
        """Puxa o sumário do materials projects apenas com os dados selecionados"""
        with MPRester(api_key) as mpr:
            self.mpr = mpr

            return mpr.summary.search(
                fields=[
                    "material_id",
                    "nelements",
                    "nsites",
                    "composition",
                    "formula_pretty",
                    "volume",
                    "density",
                    "density_atomic",
                    "symmetry",
                    "material_id",
                    "is_stable",
                    "is_magnetic",
                    "is_metal",
                    "is_gap_direct",
                    "energy_per_atom",
                    "efermi",
                    "total_magnetization",
                    "last_updated",
                    "deprecated",
                    "structure",
                ]
            )

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


class MaterialsReader:
    cols = {
        "id": int,
        "material_id": str,
        "formula": str,
        "n_elements": int,
        "n_atoms": int,
        "composition": str,
        "volume": float,
        "density": float,
        "atomic_density": float,
        "symetry": str,
        "symetry_symbol": str,
        "symetry_number": int,
        "is_stable": bool,
        "is_magnetic": bool,
        "is_metal": bool,
        "is_gap_direct": bool,
        "energy_per_atom": float,
        "efermi": float,
        "total_magnetization": float,
        "lattice_structure": str,
        "element_coords": str,
        "dos_at_efermi": float,
        "elements_dos_at_efermi": str,
        "last_updated": datetime,
        "deprecated": bool,
    }

    def read_sql(self, query) -> pd.DataFrame:
        """
        Read the SQL query and creates a DataFrame
        """
        with sqlite3.connect("mp_database.db") as conn:
            df = pd.read_sql_query(query, conn)
        return df

    def read_mp(
        self,
        columns: Optional[List[str] | None] = None,
        id: Optional[List[int] | None] = None,
        material_id: Optional[List[str] | str | None] = None,
        formula: Optional[List[str] | str | None] = None,
        n_elements: Optional[List[int] | None] = None,
        n_atoms: Optional[List[int] | None] = None,
        composition: Optional[List[str] | str | None] = None,
        volume: Optional[List[float] | None] = None,
        density: Optional[List[float] | None] = None,
        atomic_density: Optional[List[float] | None] = None,
        symetry: Optional[List[str] | str | None] = None,
        symetry_symbol: Optional[List[str] | str | None] = None,
        symetry_number: Optional[List[int] | None] = None,
        is_stable: Optional[bool | None] = None,
        is_magnetic: Optional[bool | None] = None,
        is_metal: Optional[bool | None] = None,
        is_gap_direct: Optional[bool | None] = None,
        energy_per_atom: Optional[List[float] | None] = None,
        efermi: Optional[List[float] | None] = None,
        total_magnetization: Optional[List[float] | None] = None,
        lattice_structure: Optional[List[str] | str | None] = None,
        element_coords: Optional[List[str] | str | None] = None,
        dos_at_efermi: Optional[List[float] | None] = None,
        elements_dos_at_efermi: Optional[List[str] | str | None] = None,
        deprecated: Optional[bool | None] = None,
    ) -> pd.DataFrame:
        """
        filter the materials table for specific results

        All parameters are optional, if none is used will return the full table
        When a list is given in any filter argument the result

        Parameters:
        - columns (list): list of desired columns.
        - id: Filter the column id.
        - material_id: Filter the column material_id.
        - formula: Filter the column formula.
        - n_elements: Filter the column n_elements.
        - n_atoms: Filter the column n_atoms.
        - composition: Filter the column composition.
        - volume: Filter the column volume.
        - density: Filter the column density.
        - atomic_density: Filter the column atomic_density.
        - symetry: Filter the column symetry.
        - symetry_symbol: Filter the column symetry_symbol.
        - symetry_number: Filter the column symetry_number.
        - is_stable: Filter the column is_stable.
        - is_magnetic: Filter the column is_magnetic.
        - is_metal: Filter the column is_metal.
        - is_gap_direct: Filter the column is_gap_direct.
        - energy_per_atom: Filter the column energy_per_atom.
        - efermi: Filter the column efermi.
        - total_magnetization: Filter the column total_magnetization.
        - lattice_structure: Filter the column lattice_structure.
        - element_coords: Filter the column element_coords.
        - dos_at_efermi: Filter the column dos_at_efermi.
        - elements_dos_at_efermi: Filter the column elements_dos_at_efermi.
        - deprecated: Filter the column deprecated.

        Returns:
        DataFrame: all materials records filtred.
        """
        loc = locals().copy()
        loc.pop("columns")
        loc.pop("self")

        # Cria as condições para cada uma das variaveis
        where = []
        for x in loc:
            if loc[x] == None:
                pass

            # Quando o filtro é de um valor numérico
            elif self.cols[x] in (int, float):
                # Se o filtro for um número, transforma em uma lista
                if not isinstance(loc[x], list):
                    loc[x] = [loc[x]]
                # Se a lista de filtros tiver tamanho 2
                ## então filtra todos os itens entre os valores da lista
                if len(loc[x]) == 2:
                    where.append(f"{x} >= {min(loc[x])} and {x} <= {max(loc[x])}")
                # se não, puxa todos os valores presentes na lista
                else:
                    where.append(f"{x} in {to_sql_list(loc[x])}")

            # Qaundo o filtro é boleano
            elif self.cols[x] == bool:
                # cria o filtro com o valor numérico do boleano
                where.append(f"{x} = {int(loc[x])}")

            # Nos outros casos de filtro (textos)
            ## Busca todos os valores que contenham o filtro
            else:
                if isinstance(loc[x], list):
                    for val in loc[x]:
                        where.append(f"{x} like '%%{val}%%'")
                else:
                    where.append(f"{x} like '%%{loc[x]}%%'")

        # Cria uma tabela com todos os filtros feitos
        return self.read_sql(
            f"""
            select {', '.join(list(columns or self.cols))}
            from materials
            {'where' if where else ''}
            {' and '.join(where)}
            """
        )
