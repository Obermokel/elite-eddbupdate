package borg.ed.eddbupdate.eddb;

import borg.ed.universe.data.Coord;
import com.google.gson.annotations.SerializedName;
import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldType;

import java.io.Serializable;
import java.util.Date;

/**
 * EdsmSystem
 *
 * @author <a href="mailto:b.guenther@xsite.de">Boris Guenther</a>
 */
public class EdsmSystem implements Serializable {

    private static final long serialVersionUID = -653638551508147476L;

    @Id
    @SerializedName("id")
    private Long id = null;
    //    @SerializedName("name")
    //    private String name = null;
    @Field(type = FieldType.Date)
    @SerializedName("date")
    private Date createdAt = null;
    @SerializedName("coords")
    private Coord coord = null;

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (this.getClass() != obj.getClass()) {
            return false;
        }
        EdsmSystem other = (EdsmSystem) obj;
        if (this.id == null) {
            if (other.id != null) {
                return false;
            }
        } else if (!this.id.equals(other.id)) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        return this.id.hashCode();
    }

    @Override
    public String toString() {
        return "#" + this.id; // + " " + this.name;
    }

    public Long getId() {
        return this.id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    //    public String getName() {
    //        return this.name;
    //    }
    //
    //    public void setName(String name) {
    //        this.name = name;
    //    }

    public Date getCreatedAt() {
        return this.createdAt;
    }

    public void setCreatedAt(Date createdAt) {
        this.createdAt = createdAt;
    }

    public Coord getCoord() {
        return this.coord;
    }

    public void setCoord(Coord coord) {
        this.coord = coord;
    }

}
