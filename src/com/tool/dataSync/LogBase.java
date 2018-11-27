package com.tool.dataSync;

import com.coupang.lms.domain.CreatableAndUpdatable;
import com.coupang.lms.domain.CreatedAtListener;
import com.coupang.lms.domain.UpdatedAtListener;
import lombok.Data;

import javax.persistence.*;
import javax.validation.constraints.NotNull;
import java.util.Date;

/**
 * Created by peterchen on 02/08/2018.
 */
@Data
@MappedSuperclass
@EntityListeners({CreatedAtListener.class, UpdatedAtListener.class})
public abstract class LogBase implements CreatableAndUpdatable {

    @Basic(optional = false)
    @NotNull
    @Column(name = "createdAt")
    @Temporal(TemporalType.TIMESTAMP)
    private Date createdAt;

    @Basic(optional = false)
    @NotNull
    @Column(name = "updatedAt")
    @Temporal(TemporalType.TIMESTAMP)
    private Date updatedAt;

    @Basic(optional = false)
    @NotNull
    @Column(name = "action")
    private int action;

}
