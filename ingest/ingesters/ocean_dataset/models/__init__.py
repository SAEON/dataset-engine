class DepthStat:
    """A simple class to hold min/max statistics for a single depth level."""

    def __init__(self, vmin: float = None, vmax: float = None):
        self.vmin = vmin
        self.vmax = vmax

    def to_dict(self):
        return {'vmin': self.vmin, 'vmax': self.vmax}


class VariableInfo:
    """A class to hold metadata for a single data variable."""

    def __init__(self, name: str = None, units: str = None):
        self.name = name
        self.units = units
        self.depth_stats = {}  # Dict[str, DepthStat]

    def to_dict(self):
        return {
            'name': self.name,
            'units': self.units,
            'depth_stats': {key: stat.to_dict() for key, stat in self.depth_stats.items()}
        }


class DatasetMetadata:
    """A class to structure and hold all metadata for a single ocean dataset."""

    def __init__(self):
        self.time_steps = None
        self.start_date = None
        self.end_date = None
        self.step_minutes = None
        self.depth_levels = []
        self.bounds = None
        self.variables = {}  # Dict[str, VariableInfo]
        self.grid_width = None
        self.grid_height = None
        self.u_min_global = None
        self.u_max_global = None
        self.v_min_global = None
        self.v_max_global = None

    def to_dict(self):
        """Converts the entire metadata object tree into a dictionary."""
        return {
            "time_steps": self.time_steps,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "step_minutes": self.step_minutes,
            "depth_levels": self.depth_levels,
            "bounds": self.bounds,
            "variables": {key: var.to_dict() for key, var in self.variables.items()},
            "grid_width": self.grid_width,
            "grid_height": self.grid_height,
            "u_min_global": self.u_min_global,
            "u_max_global": self.u_max_global,
            "v_min_global": self.v_min_global,
            "v_max_global": self.v_max_global,
        }