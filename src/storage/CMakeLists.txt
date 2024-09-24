add_subdirectory(disk)
add_subdirectory(index)
add_subdirectory(page)
add_subdirectory(table)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES}
        PARENT_SCOPE)
