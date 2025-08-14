import json
from psycopg2.extensions import AsIs
import numpy as np


class GridCell:
    lat_point_1: float
    lon_point_1: float

    lat_point_2: float
    lon_point_2: float

    lat_point_3: float
    lon_point_3: float

    lat_point_4: float
    lon_point_4: float

    temp_val: float
    salt_val: float
    u_val: float
    v_val: float

    def is_fully_populated(self):
        """
        Checks if all fields of the grid cell are populated (not NaN).
        Returns True if all fields are valid, False otherwise.
        """
        fields = [
            self.lat_point_1, self.lon_point_1,
            self.lat_point_2, self.lon_point_2,
            self.lat_point_3, self.lon_point_3,
            self.lat_point_4, self.lon_point_4,
            self.temp_val, self.salt_val,
            self.u_val, self.v_val
        ]

        for field_val in fields:
            if np.isnan(field_val):
                return False
        return True

    def get_cell_vertices_json(self):
        cell_vertices = [
            [float(self.lon_point_1), float(self.lat_point_1)],
            [float(self.lon_point_2), float(self.lat_point_2)],
            [float(self.lon_point_3), float(self.lat_point_3)],
            [float(self.lon_point_4), float(self.lat_point_4)]
        ]

        return json.dumps(cell_vertices)

    def get_cell_vertices_geometry(self):
        """
        Generates a Well-Known Text (WKT) string for a polygon from the cell's vertices.
        """
        vertices = [
            (float(self.lon_point_1), float(self.lat_point_1)),
            (float(self.lon_point_2), float(self.lat_point_2)),
            (float(self.lon_point_3), float(self.lat_point_3)),
            (float(self.lon_point_4), float(self.lat_point_4)),
            (float(self.lon_point_1), float(self.lat_point_1))  # Close the polygon
        ]

        wkt_points = ", ".join([f"{lon} {lat}" for lon, lat in vertices])
        return AsIs(f"ST_SetSRID(ST_GeomFromText('POLYGON(({wkt_points}))'), 4326)")


class NetcdfFileData:
    times: []
    depths: []
    lons_rho: []
    lats_rho: []
    mask: []
    temps: []
    salts: []
    us: []
    vs: []
    lon_psi: []
    lat_psi: []
    num_times: int
    num_depths: int
    num_eta: int
    num_xi: int

    def set_dimensions(self, times, depths, eta_rhos, xi_rhos):
        self.times = times
        self.depths = depths
        self.num_times = len(times)
        self.num_depths = len(depths)
        self.num_eta = len(eta_rhos)
        self.num_xi = len(xi_rhos)

    def set_coordinates(self, longitudes_rho, latitudes_rho):
        self.lons_rho = longitudes_rho
        self.lats_rho = latitudes_rho
        self.lat_psi = rho_to_psi(self.lats_rho)
        self.lon_psi = rho_to_psi(self.lons_rho)

    def get_grid_cell(self, eta_index, xi_index):
        grid_cell = GridCell()

        grid_cell.lon_point_1 = self.lon_psi[eta_index, xi_index]
        grid_cell.lat_point_1 = self.lat_psi[eta_index, xi_index]

        grid_cell.lon_point_2 = self.lon_psi[eta_index, xi_index + 1]
        grid_cell.lat_point_2 = self.lat_psi[eta_index, xi_index + 1]

        grid_cell.lon_point_3 = self.lon_psi[eta_index + 1, xi_index + 1]
        grid_cell.lat_point_3 = self.lat_psi[eta_index + 1, xi_index + 1]

        grid_cell.lon_point_4 = self.lon_psi[eta_index + 1, xi_index]
        grid_cell.lat_point_4 = self.lat_psi[eta_index + 1, xi_index]

        return grid_cell


def rho_to_psi(var_rho):
    """
    Calculates psi-grid values from rho-grid values by averaging the 4 surrounding rho-grid points.
    var_rho: A 2D numpy array of values on the rho-grid (e.g., lon_rho, lat_rho).
    Returns a 2D numpy array of values on the psi-grid, which is one smaller in each dimension.
    """
    var_psi = 0.25 * (var_rho[:-1, :-1] +
                      var_rho[1:, :-1] +
                      var_rho[:-1, 1:] +
                      var_rho[1:, 1:])
    return var_psi


class VariableThreshold:
    """
    Model for keeping track of the thresholds of a specific variable.
    """
    variable_name: str
    min_value: float
    max_value: float

    def __init__(self, variable_name: str, min_value: float = 1000, max_value: float = -1000):
        self.variable_name = variable_name
        self.min_value = min_value
        self.max_value = max_value

    def check_set_thresholds(self, check_value: float):
        if check_value < self.min_value:
            self.min_value = check_value
        elif check_value > self.max_value:
            self.max_value = check_value
