package borg.ed.eddbupdate.eddb;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Date;
import java.util.List;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class EddbBody implements Serializable {

	private static final long serialVersionUID = -7598896116401420130L;

	private Long id = null;
	private Date created_at = null;
	private Date updated_at = null;
	private String name = null;
	private Long system_id = null;
	private String type_name = null; // PlanetClass
	private BigDecimal distance_to_arrival = null;
	private String spectral_class = null; // StarClass
	private BigDecimal surface_temperature = null;
	private BigDecimal age = null;
	private BigDecimal solar_masses = null;
	private BigDecimal solar_radius = null;
	private String volcanism_type_name = null;
	private String atmosphere_type_name = null;
	private String terraforming_state_name = null;
	private BigDecimal earth_masses = null;
	private BigDecimal radius = null;
	private BigDecimal gravity = null;
	private BigDecimal surface_pressure = null;
	private BigDecimal orbital_period = null;
	private BigDecimal semi_major_axis = null;
	private BigDecimal orbital_eccentricity = null;
	private BigDecimal orbital_inclination = null;
	private BigDecimal arg_of_periapsis = null;
	private BigDecimal rotational_period = null;
	private Boolean is_rotational_period_tidally_locked = null;
	private BigDecimal axis_tilt = null;
	private List<EddbBody.Ring> rings = null;
	private List<EddbBody.AtmosphereShare> atmosphere_composition = null;
	//	private List solid_composition = null;
	private List<EddbBody.MaterialsShare> materials = null;
	private Boolean is_landable = null;

	@Getter
	@Setter
	public static class Ring implements Serializable {

		private static final long serialVersionUID = -990890046980078995L;

		private String name = null;
		private String ring_type_name = null;
		private BigDecimal ring_mass = null;
		private BigDecimal ring_inner_radius = null;
		private BigDecimal ring_outer_radius = null;

	}

	@Getter
	@Setter
	public static class AtmosphereShare implements Serializable {

		private static final long serialVersionUID = 6408807519133050851L;

		private String atmosphere_component_name = null;
		private BigDecimal share = null;

	}

	@Getter
	@Setter
	public static class MaterialsShare implements Serializable {

		private static final long serialVersionUID = 3551096389702313928L;

		private String material_name = null;
		private BigDecimal share = null;

	}

}
