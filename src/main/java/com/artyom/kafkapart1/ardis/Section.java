package com.artyom.kafkapart1.ardis;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.SuperBuilder;

@Getter
@Setter
@SuperBuilder
@NoArgsConstructor
public abstract class Section {
    private View view;
    private String id;
    private String title;
    private String header;
    private String sectionId;

    protected Section(View view, String id, String title, String sectionId) {
        this.view = view;
        this.id = id;
        this.title = title;
        this.sectionId = sectionId;
    }

    protected Section(View view, String id, String title, String sectionId, String header) {
        this.view = view;
        this.id = id;
        this.title = title;
        this.header = header;
        this.sectionId = sectionId;
    }

    protected Section(String id, String sectionId) {
        this.id = id;
        this.sectionId = sectionId;
    }
}
