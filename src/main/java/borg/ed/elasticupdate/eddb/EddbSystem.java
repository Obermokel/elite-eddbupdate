package borg.ed.elasticupdate.eddb;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Date;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class EddbSystem implements Serializable {

	private static final long serialVersionUID = -6370818296371268961L;

	private Long id = null;
	private Long edsm_id = null;
	private String name = null;
	private Float x = null;
	private Float y = null;
	private Float z = null;
	private BigDecimal population = null;
	private String government = null;
	private String allegiance = null;
	private String state = null;
	private String security = null;
	private String primary_economy = null;
	//	private String power = null;
	//	private String power_state = null;
	private Boolean needs_permit = null;
	private Date updated_at = null;
	//	private String controlling_minor_faction = null;
	private String reserve_type = null;
	//	private List<Object> minor_faction_presences = null;

}
